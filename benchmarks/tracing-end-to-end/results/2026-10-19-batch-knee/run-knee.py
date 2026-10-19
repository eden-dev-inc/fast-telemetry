from pathlib import Path
import argparse,csv,io,subprocess
from admission import gate
ROOT=Path(__file__).resolve().parent
p=argparse.ArgumentParser()
p.add_argument('--binary',required=True)
p.add_argument('--output',required=True)
p.add_argument('--affinity',default='0-15')
args=p.parse_args()
out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
sizes=[64,128,256,384,512,768,1024]
with (out/'all-runs.csv').open('w',newline='') as f:
 writer=None;ordinal=0
 for repeat in range(6):
  order=sizes[repeat%3:]+sizes[:repeat%3]
  if repeat>=3:order=list(reversed(order))
  for workload in ['root','children','attributes']:
   for batch in order:
    cycles=196608//(8*batch)
    assert cycles*8*batch==196608
    gate(out,f'knee-{ordinal:03}')
    command=['taskset','-c',args.affinity,args.binary,'native',workload,'8',str(batch),str(cycles),'16','capture']
    p=subprocess.run(command,capture_output=True,text=True,timeout=180)
    stem=out/f'{ordinal:03}-{workload}-{batch}';stem.with_suffix('.stdout').write_text(p.stdout);stem.with_suffix('.stderr').write_text(p.stderr)
    assert p.returncode==0,(command,p.stderr)
    rows=list(csv.DictReader(io.StringIO(p.stdout)));assert len(rows)==1
    row=dict(ordinal=ordinal,repetition=repeat,**rows[0]);assert int(row['verified_spans'])==196608 and int(row['wire_requests'])==cycles
    assert int(row['sampled_out'])==int(row['dropped'])==0
    if writer is None:writer=csv.DictWriter(f,fieldnames=list(row),lineterminator='\n');writer.writeheader()
    writer.writerow(row);f.flush();ordinal+=1
  print(f'repetition {repeat+1}/6: {ordinal} successful processes',flush=True)
