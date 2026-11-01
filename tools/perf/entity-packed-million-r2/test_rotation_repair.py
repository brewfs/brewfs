"""Exercise a real Linux cross-filesystem copy, including intact source evidence."""
import hashlib,json,os,tempfile,unittest
from pathlib import Path
from object_agent_repaired import copy_closed

class Rotation(unittest.TestCase):
    def test_cross_filesystem_evidence(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as first,tempfile.TemporaryDirectory(dir='/dev/shm') as second:
            source=Path(first)/'closed';target=Path(second)/'archived'
            self.assertNotEqual(Path(first).stat().st_dev,Path(second).stat().st_dev)
            body=b''.join((json.dumps(dict(method='PUT',status=200,sequence=n))+'\n').encode() for n in range(10000))
            source.write_bytes(body)
            with self.assertRaises(OSError) as failure:os.replace(source,target)
            self.assertEqual(failure.exception.errno,18)
            result=copy_closed(source,target)
            self.assertEqual(source.read_bytes(),body);self.assertEqual(target.read_bytes(),body)
            self.assertEqual(result['sha256'],hashlib.sha256(body).hexdigest())
            self.assertEqual(len(target.read_bytes().splitlines()),10000)
            with self.assertRaises(AssertionError):copy_closed(source,target)
            self.assertEqual(target.read_bytes(),body)

if __name__=='__main__':unittest.main()
