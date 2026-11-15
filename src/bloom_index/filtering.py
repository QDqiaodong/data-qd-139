import hashlib,math
from fractions import Fraction

def positions(item,bits,hashes):
    return [int.from_bytes(hashlib.sha256(str(i).encode('ascii')+bytes([0])+item.encode('utf-8')).digest(),'big')%bits for i in range(hashes)]

def audit(bits,hashes,items,queries):
    bitmap=bytearray((bits+7)//8);exact=set(items)
    for item in sorted(exact):
        for p in positions(item,bits,hashes):bitmap[p//8]|=1<<(p%8)
    result=[];false_positive=negatives=false_negative=0
    for q in queries:
        maybe=all(bitmap[p//8] & (1<<(p%8)) for p in positions(q,bits,hashes));actual=q in exact
        negatives+=not actual;false_positive+=maybe and not actual;false_negative+=actual and not maybe
        result.append({'item':q,'maybe_present':maybe,'actual_present':actual})
    return {'unique_items':len(exact),'duplicate_insertions':len(items)-len(exact),'set_bits':sum(b.bit_count() for b in bitmap),'bitmap_hex':bitmap.hex(),'queries':result,'negative_queries':negatives,'false_positives':false_positive,'false_negatives':false_negative,'observed_false_positive_rate':str(Fraction(false_positive,negatives)) if negatives else None,'idealized_false_positive_rate':(-math.expm1(-hashes*len(exact)/bits))**hashes}
