#!/usr/bin/env python3
"""Offline only: process profiles after all competing benchmark jobs stop."""
import argparse, collections, json, pathlib, re, subprocess

ROOT=pathlib.Path('/home/hxy/perf-full-20261101')
COLLAPSE='/home/hxy/.cargo/bin/inferno-collapse-perf'
FLAME='/home/hxy/.cargo/bin/inferno-flamegraph'
SYMBOL_OPTIONS=['--symfs',str(ROOT/'symbols'),'--kallsyms',str(ROOT/'environment/kallsyms.txt')]

def write_json(path,obj): path.write_text(json.dumps(obj,indent=2)+'\n')

def render(folded,svg,title):
    with svg.open('w') as out,(svg.parent/'flamegraph.log').open('w') as err:
        subprocess.run([FLAME,'--title',title,'--countname','CPU ns',str(folded)],stdout=out,stderr=err,check=True)

def demangle_folded(raw,folded):
    rows=[];names=set()
    for line in raw.read_text().splitlines():
        stack,weight=line.rsplit(' ',1);frames=stack.split(';')
        rows.append((frames,int(weight)));names.update(frames)
    names=sorted(names)
    decoded=subprocess.run(['c++filt','-s','rust'],input='\n'.join(names)+'\n',text=True,
                           capture_output=True,check=True).stdout.splitlines()
    if len(decoded)!=len(names):raise RuntimeError(f'folded demangler line count: {raw}')
    # A Rust array type may contain ';'. It is part of one symbol, not a frame separator.
    mapping={old:new.replace(';','；') for old,new in zip(names,decoded)}
    collapsed=collections.Counter()
    for frames,weight in rows:
        stack=';'.join(mapping[name] for name in frames)
        if len(stack.split(';'))!=len(frames):raise RuntimeError(f'frame boundary changed: {raw}')
        collapsed[stack]+=weight
    with folded.open('w') as out:
        for stack,weight in sorted(collapsed.items()):out.write(f'{stack} {weight}\n')
    return sum(';' in name for name in decoded)

def process(directory):
    target=directory/'profile.json'
    if target.exists(): return json.loads(target.read_text())
    data=directory/'cpu.data'
    with (directory/'perf-report.txt').open('w') as out:
        subprocess.run(['perf','report',*SYMBOL_OPTIONS,'--stdio','--no-children','--no-inline','-t','|','-g','none','--sort','overhead,symbol','--fields',
                        'overhead,sample,period,symbol','--percent-limit','0','-i',str(data)],stdout=out,check=True)
    text=(directory/'perf-report.txt').read_text(); symbols=[]
    for line in text.splitlines():
        fields=[x.strip() for x in line.split('|')]
        if len(fields)<4 or not re.fullmatch(r'[0-9.]+%',fields[0]):continue
        symbols.append({'symbol':fields[3],'self_pct':float(fields[0][:-1]),'samples':int(fields[1]),'period':int(fields[2])})
    if not symbols: raise RuntimeError(f'no samples: {data}')
    demangled=subprocess.run(['c++filt','-s','rust'],input='\n'.join(x['symbol'] for x in symbols)+'\n',
                             text=True,capture_output=True,check=True).stdout.splitlines()
    if len(demangled)!=len(symbols):raise RuntimeError(f'demangler line count: {directory}')
    for row,symbol in zip(symbols,demangled):row['raw_symbol']=row['symbol'];row['symbol']=symbol
    symbols.sort(key=lambda row:row['period'],reverse=True)
    with (directory/'cpu.raw.folded').open('w') as out,(directory/'perf-script.log').open('w') as err:
        script=subprocess.Popen(['perf','script',*SYMBOL_OPTIONS,'--no-inline','-i',str(data)],stdout=subprocess.PIPE,stderr=err)
        collapse=subprocess.run([COLLAPSE],stdin=script.stdout,stdout=out,stderr=err)
        script.stdout.close()
        if script.wait() or collapse.returncode: raise RuntimeError(f'collapse failed: {directory}')
    escaped=demangle_folded(directory/'cpu.raw.folded',directory/'cpu.folded')
    render(directory/'cpu.folded',directory/'cpu.svg',' / '.join(directory.parts[-3:]))
    period=sum(x['period'] for x in symbols); samples=sum(x['samples'] for x in symbols)
    for row in symbols:row['self_pct']=100*row['period']/period
    folded_period=sum(int(line.rsplit(' ',1)[1]) for line in (directory/'cpu.folded').read_text().splitlines())
    if folded_period!=period:raise RuntimeError(f'folded/report CPU-period mismatch: {directory}')
    unknown=sum(x['period'] for x in symbols if '[unknown]' in x['symbol'] or re.match(r'\[.\] 0x',x['symbol']))
    match=re.search(r'Total Lost Samples:\s*(\d+)',text)
    result={'samples':samples,'period':period,'lost_samples':int(match[1]) if match else None,
            'unknown_self_pct':100*unknown/period,'top20':symbols[:20],'all_symbols':symbols,
            'sources':[str(data),str(directory/'perf-report.txt')], 'event':'cpu-clock',
            'symbol_options':SYMBOL_OPTIONS,'inline_expansion':False,'rust_demangler':'GNU c++filt -s rust',
            'folded_weight_unit':'cpu-clock nanoseconds','folded_period':folded_period,
            'escaped_symbol_semicolons':escaped,'frame_structure_verified':True,
            'self_definition':'period-weighted leaf-symbol CPU share, --no-children; not inclusive stack cost'}
    write_json(target,result)
    with (directory/'top20.tsv').open('w') as out:
        out.write('self_pct\tsamples\tsymbol\n')
        for x in symbols[:20]:out.write(f'{x["self_pct"]:.2f}\t{x["samples"]}\t{x["symbol"]}\n')
    return result

p=argparse.ArgumentParser();p.add_argument('--mode');p.add_argument('--workload');p.add_argument('--refresh-folded',action='store_true');a=p.parse_args()
for complete in sorted((ROOT/'results').glob('*/*/complete.json')):
    directory=complete.parent; mode,name=directory.parts[-2:]
    if a.mode and mode!=a.mode: continue
    if a.workload and name!=a.workload: continue
    if a.refresh_folded:
        all_frames=collections.Counter()
        for r in range(1,4):
            current=directory/f'measure-{r}'
            escaped=demangle_folded(current/'cpu.raw.folded',current/'cpu.folded')
            profile=json.loads((current/'profile.json').read_text())
            period=0
            for line in (current/'cpu.folded').read_text().splitlines():
                stack,weight=line.rsplit(' ',1);weight=int(weight);period+=weight;all_frames[stack]+=weight
            if period!=profile['period']:raise RuntimeError(f'CPU period mismatch: {current}')
            profile.update(escaped_symbol_semicolons=escaped,frame_structure_verified=True,
                           folded_period=period,folded_weight_unit='cpu-clock nanoseconds')
            write_json(current/'profile.json',profile)
            render(current/'cpu.folded',current/'cpu.svg',' / '.join(current.parts[-3:]))
        with (directory/'cpu.folded').open('w') as output:
            for stack,weight in sorted(all_frames.items()):output.write(f'{stack} {weight}\n')
        render(directory/'cpu.folded',directory/'cpu.svg',f'{mode} / {name}: all 3 measurement rounds')
        print(f'refreshed frame-preserving folded/SVG {mode}/{name}',flush=True)
        continue
    if (directory/'profile.json').exists(): continue
    summaries=[]; folded=collections.Counter(); symbols=collections.defaultdict(lambda:{'samples':0,'period':0})
    for r in range(1,4):
        current=directory/f'measure-{r}'; summary=process(current); summaries.append(summary)
        for x in summary['all_symbols']:
            symbols[x['symbol']]['samples']+=x['samples'];symbols[x['symbol']]['period']+=x['period']
        for line in (current/'cpu.folded').read_text().splitlines():
            stack,count=line.rsplit(' ',1);folded[stack]+=int(count)
    period=sum(s['period'] for s in summaries)
    ranked=[{'symbol':symbol,**values,'self_pct':100*values['period']/period}
            for symbol,values in sorted(symbols.items(),key=lambda x:x[1]['period'],reverse=True)]
    with (directory/'cpu.folded').open('w') as output:
        for stack,count in sorted(folded.items()):output.write(f'{stack} {count}\n')
    render(directory/'cpu.folded',directory/'cpu.svg',f'{mode} / {name}: all 3 measurement rounds')
    result={'samples':sum(s['samples'] for s in summaries),'period':period,
            'lost_samples':sum(s['lost_samples'] or 0 for s in summaries),
            'unknown_self_pct':sum(s['unknown_self_pct']*s['period'] for s in summaries)/period,
            'top20':ranked[:20],'all_symbols':ranked,
            'source_profiles':[str(directory/f'measure-{r}/profile.json') for r in range(1,4)],
            'aggregation':'pool CPU sample periods from all 3 measurement rounds; no selection by throughput'}
    write_json(directory/'profile.json',result)
    with (directory/'top20.tsv').open('w') as output:
        output.write('self_pct\tsamples\tsymbol\n')
        for x in ranked[:20]:output.write(f'{x["self_pct"]:.4f}\t{x["samples"]}\t{x["symbol"]}\n')
    print(f'profile complete {mode}/{name}: {result["samples"]} samples',flush=True)
