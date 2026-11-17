"""Host mount boundaries; never read secret contents or start services."""
from pathlib import Path
import os
from processing.e2e.guards import require

SERVICE_ROLES = {'e2e-airflow-init':'init','e2e-airflow-api':'api',
                 'e2e-airflow-dag-processor':'dag-processor',
                 'e2e-airflow-scheduler':'scheduler','e2e-api':'fastapi'}
SOURCE_DIRS = ('processing','scripts','configs','config','messaging','ingestion','storage','api','infra/airflow/e2e_dags')
SECRETS = {'worker_runtime':'worker.json','control_runtime':'control.json',
           'api_runtime':'api.json','airflow_passwords':'airflow_passwords.json'}
SERVICE_SECRETS = {'init':{'worker_runtime','airflow_passwords'},
                   'api':{'control_runtime','airflow_passwords'},
                   'dag-processor':{'control_runtime','airflow_passwords'},
                   'scheduler':{'worker_runtime','airflow_passwords'},'fastapi':{'api_runtime'}}

def layout_for(rid):
    return {'version':1,'host_control':'output/e2e_control/'+rid,
            'host_private':'output/e2e_private/'+rid,'host_runtime':'output/e2e_runtime/'+rid,
            'container_control':'/opt/e2e/control','container_bootstrap':'/opt/e2e/bootstrap',
            'container_service_logs':'/opt/e2e/service-logs'}

def host_paths(m, root):
    require(m.get('runtime_layout') == layout_for(m['resources']['rid']), 'RUNTIME_LAYOUT_BINDING')
    root=Path(root).resolve(); result={k:(root/m['runtime_layout']['host_'+k]).absolute() for k in ('control','private','runtime')}
    for p in result.values():
        require(p.resolve().is_relative_to(root),'LAYOUT_ESCAPE')
        reject_links(p)
    require(all(not contains(a,b) and not contains(b,a) for i,a in enumerate(result.values()) for b in list(result.values())[i+1:]),'LAYOUT_ROOT_OVERLAP')
    return result

def contains(parent, child):
    return Path(child).resolve().is_relative_to(Path(parent).resolve())

def reject_links(path, recursive=False):
    """Reject symlinks, Windows junction/reparse aliases, and multiply-linked files."""
    path=Path(path).absolute()
    def check(p):
        if not p.exists() and not p.is_symlink():return
        s=p.lstat()
        require(not p.is_symlink() and not (getattr(s,'st_file_attributes',0)&0x400),'MOUNT_REPARSE_OR_SYMLINK')
        require(not p.is_file() or s.st_nlink==1,'MOUNT_FILE_HARDLINK')
    for p in [path,*path.parents]:check(p)
    if recursive and path.is_dir():
        for current,dirs,files in os.walk(path,followlinks=False):
            for name in dirs+files:check(Path(current)/name)

def expected_binds(m, root, manifest_path, approval):
    paths=host_paths(m,root);root=Path(root).resolve();l=m['runtime_layout']
    common=[(root/s,'/opt/project/'+s,True) for s in SOURCE_DIRS]
    common += [(Path(manifest_path),'/opt/e2e/manifest.json',True),(Path(approval),'/opt/e2e/approval.json',True)]
    result={}
    for service,role in SERVICE_ROLES.items():
        binds=common+([] if role=='init' else [(paths['runtime']/'services'/role,l['container_service_logs'],False)])
        if role!='fastapi':binds += [(root/'infra/airflow/e2e_dags','/opt/airflow/e2e_dags',True)]
        if role=='init':binds += [(paths['control'],l['container_control'],True),(paths['runtime']/'bootstrap',l['container_bootstrap'],False)]
        if role=='scheduler':binds += [(paths['runtime']/'pipeline',m['container_evidence'],False),
                                      (paths['runtime']/'bootstrap/provision.json',m['container_evidence']+'/provision.json',True),
                                      (Path(m['host_raw']),'/opt/e2e/input/raw.jsonl',True),(Path(m['host_archive']),'/opt/e2e/input/probes.json.bz2',True)]
        result[service]=binds
    return result

def validate_mounts(m, root, config, manifest_path, approval, require_files=True):
    paths=host_paths(m,root);expect=expected_binds(m,root,manifest_path,approval)
    require(config['name']==m['resources']['project'] and set(config['services'])==set(SERVICE_ROLES),'LAYOUT_SERVICE_SET')
    require(Path(approval).resolve()==(paths['control']/'approval.json').resolve(),'APPROVAL_PATH_NOT_CONTROL')
    secret_paths={name:paths['private']/file for name,file in SECRETS.items()}
    expected_volume_names={'home_'+x.replace('-','_'):m['resources']['project']+'-'+x+'-home' for x in ('init','api','dag-processor','scheduler')}
    expected_volume_names['spark_scratch']=m['resources']['project']+'-spark-scratch'
    require(set(config['volumes'])==set(expected_volume_names),'COMPUTE_VOLUME_SET')
    for name,value in expected_volume_names.items():
        v=config['volumes'][name]
        require(v.get('name')==value and not v.get('external',False) and not v.get('driver_opts') and v.get('driver','local')=='local','COMPUTE_VOLUME_HOST_ALIAS')
    require(set(config['secrets'])==set(SECRETS),'SECRET_DEFINITION_SET')
    for name,path in secret_paths.items():
        reject_links(Path(config['secrets'][name]['file']))
        require(Path(config['secrets'][name]['file']).resolve()==path.resolve(),'SECRET_PATH_BINDING')
        reject_links(path)
        if require_files:require(path.is_file(),'SECRET_FILE_NOT_PREPARED')
    for service,role in SERVICE_ROLES.items():
        s=config['services'][service]
        require(s.get('read_only') is True and not s.get('privileged',False),'ROOTFS_OR_PRIVILEGED')
        require(s.get('user')=='50000:0' and not s.get('cap_add') and not s.get('env_file'),'SERVICE_USER_OR_ENV_FILE')
        require('ALL' in s.get('cap_drop',[]) and 'no-new-privileges:true' in s.get('security_opt',[]),'SERVICE_PRIVILEGES')
        require(s.get('image')==m['image'],'LAYOUT_IMAGE')
        for mount in s['volumes']:
            if mount['type']=='bind':reject_links(Path(mount['source']),recursive=True)
        actual={(str(Path(x['source']).resolve()),x['target'],x.get('read_only',False)) for x in s['volumes'] if x['type']=='bind'}
        required={(str(p.resolve()),dest,ro) for p,dest,ro in expect[service]}
        require(actual==required,'MOUNT_ALLOWLIST')
        allowed_volumes=set() if role=='fastapi' else {'home_'+role.replace('-','_')}
        if role=='scheduler':allowed_volumes.add('spark_scratch')
        actual_volumes={(x['source'],x['target'],x.get('read_only',False)) for x in s['volumes'] if x['type']=='volume'}
        expected_volumes={(v,'/opt/e2e/scratch' if v=='spark_scratch' else '/opt/airflow',False) for v in allowed_volumes}
        require(actual_volumes==expected_volumes and all(x['type'] in ('bind','volume') for x in s['volumes']),'NAMED_VOLUME_ALLOWLIST')
        require({x['source'] for x in s.get('secrets',[])}==SERVICE_SECRETS[role],'SERVICE_SECRET_ALLOWLIST')
        require({(x['source'],x['target']) for x in s.get('secrets',[])}=={(x,'airflow_passwords.json' if x=='airflow_passwords' else 'runtime.json') for x in SERVICE_SECRETS[role]},'SECRET_TARGET_ALLOWLIST')
        for source,target,ro in expect[service]:
            reject_links(source,recursive=True)
            require(not any(contains(source,p) for p in secret_paths.values()),'SECRET_ANCESTOR_BIND')
            if not ro:require(not contains(source,paths['control']) and not contains(paths['control'],source),'WRITABLE_CONTROL_ALIAS')
        require(all(not config['volumes'][v].get('external',False) for v in allowed_volumes),'EXTERNAL_COMPUTE_VOLUME')
    return {'services':5,'secret_ancestor_binds':0,'writable_control_aliases':0,'static_only':True}
