from .common import require,ident,number,rows
from .filtering import audit

def analyze(data):
    require(data,'bits hashes items queries');bits=number(data['bits'],1,1000000);hashes=number(data['hashes'],1,16)
    items=[ident(i) for i in rows(data['items'],20000)];queries=[ident(i) for i in rows(data['queries'],20000)]
    return audit(bits,hashes,items,queries)
