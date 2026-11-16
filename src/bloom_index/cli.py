DESCRIPTION='Deterministic bounded Bloom membership with exact audit evidence'
import argparse, json, sqlite3, sys
from pathlib import Path
from .common import read, save
from .model import analyze
from . import store

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

COMMANDS={'init':init,'sync':sync,'query':query}

def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv)
    try:
        if argv and argv[0] in COMMANDS:return COMMANDS[argv[0]](argv[1:])
        return legacy(argv)
    except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError,sqlite3.Error) as error:
        print(str(error),file=sys.stderr);return 2

if __name__=='__main__':raise SystemExit(main())
