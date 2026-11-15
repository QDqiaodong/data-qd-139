
import copy,json,unittest,tempfile,subprocess,sys,hashlib,random,itertools
from pathlib import Path
from fractions import Fraction
from bloom_index.model import analyze
ROOT=Path(__file__).resolve().parents[1]
def sample():return json.loads((ROOT/'fixtures/input.json').read_text())
class Business(unittest.TestCase):
    def bad(self,d):
        with self.assertRaises((ValueError,TypeError)):analyze(d)
    def test_unknown_fields(self):
        d=sample();d['unknown']=1;self.bad(d)

    def test_membership(self):
        r=analyze(sample());self.assertEqual(r['unique_items'],3);self.assertEqual(r['duplicate_insertions'],1);self.assertEqual(r['false_negatives'],0);self.assertTrue(r['queries'][0]['maybe_present']);self.assertEqual(len(r['bitmap_hex']),16)
    def test_forced_false_positive(self):
        d={'bits':1,'hashes':1,'items':['a'],'queries':['a','b','b']};r=analyze(d);self.assertEqual(r['false_positives'],2);self.assertEqual(r['observed_false_positive_rate'],'1');self.assertEqual(r['bitmap_hex'],'01')
    def test_empty(self):
        r=analyze({'bits':9,'hashes':2,'items':[],'queries':['a']});self.assertEqual(r['set_bits'],0);self.assertFalse(r['queries'][0]['maybe_present']);self.assertEqual(r['idealized_false_positive_rate'],0)
    def test_no_negative_denominator(self):
        d=sample();d['queries']=['alpha'];self.assertIsNone(analyze(d)['observed_false_positive_rate'])
    def test_order(self):
        d=sample();d['items'].reverse();self.assertEqual(analyze(d),analyze(sample()))
    def test_no_false_negative_many(self):
        items=[str(i) for i in range(300)];r=analyze({'bits':997,'hashes':4,'items':items,'queries':items});self.assertEqual(r['false_negatives'],0);self.assertTrue(all(q['maybe_present'] for q in r['queries']))
    def test_hash_contract(self):
        from bloom_index.filtering import positions
        expected=int.from_bytes(hashlib.sha256(b'0'+bytes([0])+b'alpha').digest(),'big')%64;self.assertEqual(positions('alpha',64,1),[expected])
    def test_invalid_bits(self):
        d=sample();d['bits']=0;self.bad(d)

class Command(unittest.TestCase):
    def call(self,source,output):
        return subprocess.run([sys.executable,'-m','bloom_index.cli','--source',str(source),'--output',str(output)],capture_output=True,text=True)
    def test_cli_replay(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td)/'out.json';src=ROOT/'fixtures/input.json';r=self.call(src,out);self.assertEqual(r.returncode,0,r.stderr);first=out.read_bytes();self.assertEqual(json.loads(first)['source_sha256'],hashlib.sha256(src.read_bytes()).hexdigest());self.assertEqual(self.call(src,out).returncode,0);self.assertEqual(first,out.read_bytes())
    def test_failure_preserves(self):
        with tempfile.TemporaryDirectory() as td:
            src=Path(td)/'bad.json';out=Path(td)/'out.json';out.write_text('prior')
            for body in ['{}','{"a":1,"a":2}','NaN']:
                src.write_text(body);self.assertEqual(self.call(src,out).returncode,2);self.assertEqual(out.read_text(),'prior')
    def test_same_path_and_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            src=Path(td)/'in.json';src.write_bytes((ROOT/'fixtures/input.json').read_bytes());before=src.read_bytes();self.assertEqual(self.call(src,src).returncode,2);self.assertEqual(src.read_bytes(),before);out=Path(td)/'link.json';out.symlink_to(src);self.assertEqual(self.call(src,out).returncode,2);self.assertEqual(src.read_bytes(),before)
