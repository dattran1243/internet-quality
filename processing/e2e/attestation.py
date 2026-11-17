"""Verify original creation evidence and a separately authorized manifest rebind."""
import json
from pathlib import Path
from processing.e2e.guards import require,file_hash
from processing.e2e.layout import reject_links

EVIDENCE_KEYS={'kafka','minio','postgres','clickhouse'}
SOURCE_CHANGES={'processing/e2e/guards.py','processing/e2e/layout.py','processing/e2e/attestation.py',
                'processing/e2e/stages.py','scripts/e2e_bootstrap.py','scripts/e2e_control.py'}
DEPLOYMENT_CHANGES={'infra/airflow/docker-compose.viq-e2e-isolated.yml','.gitignore',
                    'scripts/prepare_viq_e2e_runtime_secrets.ps1'}
RESOURCE_COUNTS={'kafka':2,'minio':3,'postgres':2,'clickhouse':6}

def verify_provenance(record):
    required={'provenance','owner_created_resources','agent_created_resources'}
    require(required<=set(record),'CREATION_PROVENANCE_REQUIRED')
    provenance=record['provenance']
    require(isinstance(provenance,dict) and set(provenance)==EVIDENCE_KEYS and
            all(isinstance(v,str) and v in ('OWNER_CREATED','AGENT_CREATED') for v in provenance.values()),
            'CREATION_PROVENANCE_SCHEMA')
    owner=record['owner_created_resources'];agent=record['agent_created_resources']
    require(type(owner) is int and type(agent) is int and owner>=0 and agent>=0,
            'CREATION_COUNTS_TYPE')
    require(owner==sum(RESOURCE_COUNTS[k] for k,v in provenance.items() if v=='OWNER_CREATED') and
            agent==sum(RESOURCE_COUNTS[k] for k,v in provenance.items() if v=='AGENT_CREATED') and
            owner+agent==13,'CREATION_COUNTS_PROVENANCE_MISMATCH')

def read_json(path):
    reject_links(path)
    return json.loads(Path(path).read_bytes())

def verify_creation(original, manifest_hash, resources, control):
    verify_provenance(original)
    require(original.get('manifest_sha256')==manifest_hash and original.get('resources')==resources and
            original.get('created_new') is True and bool(original.get('operator')),'ORIGINAL_CREATION_ATTESTATION')
    require(set(original.get('evidence_files',{}))==EVIDENCE_KEYS,'ADMIN_CREATION_EVIDENCE_MISSING')
    for artifact in original['evidence_files'].values():
        relative=Path(artifact['path']);p=control/relative
        require(not relative.is_absolute() and ':' not in artifact['path'] and '\\' not in artifact['path'] and
                artifact['path'].startswith('admin/') and '..' not in relative.parts and
                p.resolve().is_relative_to((control/'admin').resolve()),'ADMIN_EVIDENCE_PATH_ESCAPE')
        reject_links(p)
        require(file_hash(p)==artifact['sha256'],'ADMIN_CREATION_EVIDENCE_BINDING')

def verify_attestation(m, manifest_path, control):
    control=Path(control);reject_links(control,recursive=True)
    mode=m['provision_attestation']['mode'];mh=file_hash(manifest_path)
    if mode=='creation_v1':
        a=read_json(control/'admin_provision.json');verify_creation(a,mh,m['resources'],control)
        return {'creation_workflow_verified':True,'resources_created_in_this_init':False,
                'admin_attestation_sha256':file_hash(control/'admin_provision.json'),
                'original_admin_attestation_sha256':file_hash(control/'admin_provision.json')}
    require(mode=='security_rebind_v2','ATTESTATION_MODE')
    expected=m['provision_attestation'];oldpath=control/'original_manifest.json';apath=control/'original_admin_provision.json'
    require(file_hash(oldpath)==expected['original_manifest_sha256'] and file_hash(apath)==expected['original_attestation_sha256'],'ORIGINAL_ATTESTATION_ANCHORS')
    old=read_json(oldpath);original=read_json(apath)
    verify_creation(original,file_hash(oldpath),old['resources'],control)
    allowed={'source_sha256','deployment_sha256','runtime_layout','provision_attestation'}
    require({k:v for k,v in old.items() if k not in allowed}=={k:v for k,v in m.items() if k not in allowed},'REBIND_CHANGED_RESOURCES_INPUT_IMAGE_OR_SCOPE')
    for name,allowed_changes in [('source_sha256',SOURCE_CHANGES),('deployment_sha256',DEPLOYMENT_CHANGES)]:
        before=old[name];after=m[name]
        changed={k for k in set(before)|set(after) if before.get(k)!=after.get(k)}
        require(set(before)<=set(after) and changed==allowed_changes,'REBIND_PIN_CHANGE_SET')
    a=read_json(control/'admin_provision.superseding.json')
    verify_provenance(a)
    require(a.get('schema_version')==2 and a.get('attestation_type')=='SECURITY_CORRECTION_REBIND' and
            a.get('manifest_sha256')==mh and a.get('resources')==m['resources'] and
            a.get('original_manifest_sha256')==file_hash(oldpath) and
            a.get('original_attestation_sha256')==file_hash(apath),'REBIND_ATTESTATION_BINDING')
    require(a.get('resources_created_in_this_correction') is False and 'created_new' not in a and
            a.get('review_only') is False and bool(a.get('operator')) and bool(a.get('recorded_at_utc')),'REBIND_NOT_AUTHORIZED_OR_FAKE_CREATION')
    require(a['provenance']==original['provenance'] and
            a['owner_created_resources']==original['owner_created_resources'] and
            a['agent_created_resources']==original['agent_created_resources'] and
            a.get('evidence_files')==original['evidence_files'],'REBIND_CHANGED_CREATION_PROVENANCE')
    return {'creation_workflow_verified':True,'resources_created_in_this_init':False,
            'admin_attestation_sha256':file_hash(control/'admin_provision.superseding.json'),
            'original_admin_attestation_sha256':file_hash(apath)}
