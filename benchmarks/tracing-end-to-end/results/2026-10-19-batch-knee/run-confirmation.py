from pathlib import Path
import argparse,csv,io,subprocess
from admission import gate
ROOT=Path(__file__).resolve().parent
p=argparse.ArgumentParser()
p.add_argument('--baseline',required=True)
p.add_argument('--candidate',required=True)
p.add_argument('--output',required=True)
p.add_argument('--affinity',default='0-15')
args=p.parse_args()
out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
base=Path(args.baseline)
candidate=Path(args.candidate)
variants=[('before',base,[]),('configured-256',candidate,['256']),('default-384',candidate,[])]
with (out/'all-runs.csv').open('w',newline='') as f:
 writer=None;ordinal=0
 for repeat in range(6):
  order=variants[repeat%3:]+variants[:repeat%3]
  if repeat>=3:order=list(reversed(order))
  for threads in [1,8]:
   for work in ['root','children','attributes']:
    for name,binary,extra in order:
     gate(out,f'confirm-{ordinal:03}')
     command=['taskset','-c',args.affinity,str(binary),'native',work,str(threads),'384',str(512//threads),'16','capture',*extra]
     p=subprocess.run(command,capture_output=True,text=True,timeout=180)
     stem=out/f'{ordinal:03}-{name}-{work}-{threads}';stem.with_suffix('.stdout').write_text(p.stdout);stem.with_suffix('.stderr').write_text(p.stderr)
     assert p.returncode==0,(command,p.stderr)
     rows=list(csv.DictReader(io.StringIO(p.stdout)));assert len(rows)==1
     row=dict(ordinal=ordinal,repetition=repeat,variant=name,**rows[0]);row.setdefault('producer_batch_size','256')
     assert int(row['verified_spans'])==196608 and int(row['wire_requests'])==512//threads
     assert int(row['producer_batch_size'])==(384 if name=='default-384' else 256)
     assert int(row['sampled_out'])==int(row['dropped'])==0
     if writer is None:writer=csv.DictWriter(f,fieldnames=list(row),lineterminator='\n');writer.writeheader()
     writer.writerow(row);f.flush();ordinal+=1
  print(f'repetition {repeat+1}/6: {ordinal} successful processes',flush=True)
