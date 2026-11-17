DESCRIPTION='Deterministic bounded Bloom membership with exact audit evidence'
import argparse, json, os, sqlite3, sys
from pathlib import Path
from .common import read, save
from .model import analyze
from . import resolve as resolve_mod, snapshot, store, window

def run(source):
    data,digest=read(source)
    return {'schema_version':1,'source_sha256':digest,'result':analyze(data)}

def legacy(argv):
    parser=argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    if args.source.resolve()==args.output.resolve():raise ValueError('input and output must differ')
    report=run(args.source);save(args.output,report)
    print(json.dumps(report,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def init(argv):
    parser=argparse.ArgumentParser(prog='bloom-index init',description='Create an empty persistent membership index')
    parser.add_argument('--db',type=Path,required=True)
    parser.add_argument('--bits',type=int,required=True)
    parser.add_argument('--hashes',type=int,required=True)
    args=parser.parse_args(argv)
    summary=store.init_db(args.db,args.bits,args.hashes)
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def sync(argv):
    parser=argparse.ArgumentParser(prog='bloom-index sync',description='Atomically replace source ownership from a control file')
    parser.add_argument('--db',type=Path,required=True)
    parser.add_argument('--source',type=Path,required=True)
    args=parser.parse_args(argv)
    if args.db.resolve()==args.source.resolve():raise ValueError('database and control file must differ')
    summary=store.sync_control(args.db,args.source)
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def query(argv):
    parser=argparse.ArgumentParser(prog='bloom-index query',description='Audit one committed snapshot of a persistent index')
    parser.add_argument('--db',type=Path,required=True)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    if args.source.resolve()==args.output.resolve():raise ValueError('input and output must differ')
    if args.db.resolve() in (args.source.resolve(),args.output.resolve()):raise ValueError('database path conflicts with input or output')
    report=store.query_db(args.db,args.source);save(args.output,report)
    print(json.dumps(report,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def snapshot_export(argv):
    parser=argparse.ArgumentParser(prog='bloom-index snapshot',description='Export one committed database state as a verifiable bundle')
    parser.add_argument('--db',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    summary=snapshot.export_snapshot(args.db,args.output)
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def snapshot_check(argv):
    parser=argparse.ArgumentParser(prog='bloom-index snapshot-check',description='Verify a snapshot bundle read-only; exit 2 on any defect')
    parser.add_argument('--bundle',type=Path,required=True)
    args=parser.parse_args(argv)
    verified=snapshot.verify_bundle(args.bundle)
    state=verified['state'];manifest=verified['manifest']
    summary={'status':'ok','format_version':manifest['format_version'],'commit_seq':state['commit_seq'],
             'bits':state['bits'],'hashes':state['hashes'],'active_items':state['active_items'],
             'sources':state['sources'],'db_size_bytes':verified['db_size_bytes'],
             'db_sha256':verified['db_sha256']}
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def restore(argv):
    parser=argparse.ArgumentParser(prog='bloom-index restore',description='Restore a verified bundle to a new database path')
    parser.add_argument('--bundle',type=Path,required=True)
    parser.add_argument('--db',type=Path,required=True)
    args=parser.parse_args(argv)
    summary=snapshot.restore_bundle(args.bundle,args.db)
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def resolve(argv):
    parser=argparse.ArgumentParser(prog='bloom-index resolve',
        description='Merge supplier master alias records into constrained entity groups on one pinned snapshot')
    parser.add_argument('--db',type=Path,required=True)
    parser.add_argument('--source',type=Path,required=True,help='strict control JSON with a files list')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    stats_path=os.environ.get('BLOOM_INDEX_RESOLVE_STATS') or None
    stats=resolve_mod.resolve(args.db,args.source,args.output,stats_path)
    summary={'commit_seq':stats['commit_seq'],'total':stats['total'],'active':stats['active'],
             'inactive':stats['inactive'],'entities':stats['entities']}
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def window_init(argv):
    parser=argparse.ArgumentParser(prog='bloom-index window-init',
        description='Create a window state permanently bound to its partitions, width and report path')
    parser.add_argument('--state',type=Path,required=True)
    parser.add_argument('--partitions',type=str,required=True,help='comma separated identifiers')
    parser.add_argument('--width',type=int,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    summary=window.init_state(args.state,args.partitions,args.width,args.output)
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def window_apply(argv):
    parser=argparse.ArgumentParser(prog='bloom-index window-apply',
        description='Apply one strict change batch, then recover and publish its report')
    parser.add_argument('--state',type=Path,required=True)
    parser.add_argument('--source',type=Path,required=True,help='strict control JSON with a files list')
    args=parser.parse_args(argv)
    summary=window.apply_control(args.state,args.source)
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def window_recover(argv):
    parser=argparse.ArgumentParser(prog='bloom-index window-recover',
        description='Finish any interrupted window report publication from the committed state')
    parser.add_argument('--state',type=Path,required=True)
    args=parser.parse_args(argv)
    summary=window.recover_state(args.state)
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

COMMANDS={'init':init,'sync':sync,'query':query,
          'snapshot':snapshot_export,'snapshot-check':snapshot_check,'restore':restore,
          'resolve':resolve,'window-init':window_init,'window-apply':window_apply,
          'window-recover':window_recover}

def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv)
    try:
        if argv and argv[0] in COMMANDS:return COMMANDS[argv[0]](argv[1:])
        return legacy(argv)
    except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError,sqlite3.Error) as error:
        print(str(error),file=sys.stderr);return 2

if __name__=='__main__':raise SystemExit(main())
