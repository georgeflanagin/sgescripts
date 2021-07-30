#! /usr/bin/env python
#
# python script to generate and execute an SGE script to run the single
# argument filename as a g16 job parsing the DefaultRoute and job file
# to determine the resources requested
#

import sys 
import os
import re
import getopt

# variables to be used

# queues is a self-dict, where the keys and values are identical.
queues = {
    'all':'all',
    'bigmem':'bigmem'
    }
qs = "|".join(sorted(queues.keys()))
queue = "all"

# versions maps "B" onto the path. The syntax here is designed to
# allow form several versions. Right now, there is only one.
versions = {'B': '/usr/local/gaussian/g16B01'}
vs = "|".join(sorted(versions.keys()))
version = "B"
execute = True

# Calling eval is kind of dangerous.
def bytes(memstr:str) -> str:
    """
    Determine the number of bytes in memstr providing memory requirments in g16.
    """
    s = memstr.strip().upper()          # make sure we only have strippped upper case
    unit = s[-1]                        # get the last letter
    if unit != 'W' and unit != 'B':     # make sure it ends with B or W, otherwise it is W
        s = s+'W'
    return eval(s.replace("B","*1").replace("W","*8").replace("G","*1024M").replace("M","*1024K").replace("K","*1024"))

def usage() -> None:
    # If we don't do this, Python will think these are local variables.
    # In fact, they are defined above.
    global vs, version, queue, qs

    print(f"""
        usage: qg16 [-h] [-n] [-q {qs}s] [-v {vs}s] g16-input-file 
            -h prints this helps and exists
            -n do not execute the job but only create the script file
            -q queue to run in. Default {queue}s.
            -v version to run. Default {version}s.
        """) 

# parse arguments
try:
    opts, args = getopt.getopt(sys.argv[1:],'hnq:v:')
except getopt.GetoptError as err:
    print(f"{err=}")
    usage()
    # Ordinarily, you should sys.exit() with a reserved symbol like os.EX_DATAERR,
    # but I don't know what program is on the receiving end of this code, so I'm
    # leaving these as explicit integers.
    sys.exit(1)

# This for-loop is outside of a function, so we don't need to 
# declare these to be global; i.e., this code is also at global
# scope. opts is a list of tuples, so we are implictly separating
# the elements of each tuple into two variables: o and a.
for o, a in opts:
    # These lines reverse the boolean value of the variables
    # if these switches appear in the command line.
    if o == '-n': execute = False
    if o == '-q': queue=a
    if o == '-v': version=a
    if o == '-h':
        usage()
        sys.exit(2)

if not queue in queues:
    print(f"Error in the -q argument value, which should be one of: {qs}")
    sys.exit(3)

if not version in versions:
    print("Error in -v argument value, which should be one of: {vs}")
    sys.exit(4)

g16root = versions[version]

# only last non option is the filename. I removed the len(args)>0 test,
# and replaced it with the more usual statement.
if len(args): 
    fname = args.pop()
else:
    print("Provide a filename")
    usage()
    sys.exit(5)

if not os.path.exists(fname):
    print (f'File {fname} does not exist. Exiting....')
    sys.exit(2)

# determine the name of job. This code looks from the RHS (rindex)
# for a dot (.), it then chops the string at that point, removing 
# the suffix. If that doesn't work, then we take job = fname in situ.
try:    
    job = fname[0:fname.rindex('.')]
except ValueError as e: 
    job = fname

# jobname=job
# if (jobname[0].isdigit()): jobname="q_"+job
jobname = f"q_{job}" if jobname[0].isdigit() else job

# the defaults for g16 without any override from Default.Route
nodes = 1
ppn = 1
mem = 6 * 1024 *1024 * 8
minmem = 0
maxdisk = 0

# determine overrides from Default.Route if it exists
# internally concatenate local to global so local is last and overrides 
# global settings
deffile = ""
defaultRoute = f"{g16root}/g16/Default.Route"

# Read these files if they exist, and concat their contents into deffile
if os.path.exists(defaultRoute): deffile = open(defaultRoute).read()
if os.path.exists('Default.Route'): deffile += open('Default.Route').read()

# If deffile has nothing in it, then we have to make some changes.
if len(deffile) > 0:
    # -M- memory default in words (1 word = 8 bytes)  equivalent to %mem
    pat = re.compile(r'-M-.*?([0-9]+[kmg][bw])',re.IGNORECASE)
    ss = map(bytes, re.findall(pat, deffile))
    if len(ss): mem = ss.pop()

    # -P- numproc shared  equivalent to %nprocshared 
    pat = re.compile(r'-P-.*?([0-9])', re.IGNORECASE)
    ss = map(int, re.findall(pat,deffile))
    if len(ss): ppn = ss.pop()

    # -#- Maxdisk=xxx
    pat = re.compile(r'-#-.*?maxdisk.*?([0-9]+[kmg][bw])',re.IGNORECASE)
    ss = map(bytes, re.findall(pat, deffile))
    if len(ss): maxdisk = ss.pop()

# determine the overrides as requested by the job itself
comfile = open(fname).read()

# the rest of the computational resources once they have been established
# need to be appended to the resources list with ppn first!!!
resources = []

# %nproc and %nprocshared requested to determine ppn=xxx
nproc = ppn                      # minimially default pnn
pat = re.compile(r'%nproc=([0-9]+)', re.IGNORECASE)
ss = map(int, re.findall(pat, comfile))
if len(ss) > 0: nproc=max(ss)    # nproc is really what the comfile wanted

#print "nproc="+str(nproc)
# %nprocshared if it exists (seemed to be part of g16 as well)
# that can override the -P-, which is really the number of processors (%nproc)
# as well
nprocs = ppn                      # minimially default pnn
pat = re.compile(r'%nprocs.*?=([0-9]+)', re.IGNORECASE)
ss = map(int, re.findall(pat, comfile))
if len(ss) > 0: nprocs=max(ss)    # nprocs is really what the comfile wanted
#print "nprocs="+str(nprocs)
ppn = max(nprocs,nproc)           # pick the higher of nprocs or nprocshared

# %mem requested to determine mem=xxxmb
pat = re.compile(r'%mem.*?=([0-9]+[kmg][bw])', re.IGNORECASE)
ss = map(bytes,re.findall(pat, comfile))
if len(ss) > 0: mem=max(ss)    # memory is really what the comfile wanted
mem = max(mem, minmem)         # but PBS seems to have a minimum requirement?
resources.append(f"mem_free={mem//(1024*1024)}M")

# maxdisk requested to determine file=xxxxmb
pat = re.compile(r'maxdisk=([0-9]+[kmg][bw])', re.IGNORECASE)
ss = map(bytes,re.findall(pat, comfile))
if len(ss): maxdisk= max(ss) # maxdisk is really what the comfile wanted
if maxdisk: resources.append("diskfree="+str(maxdisk/(1024*1024))+"M")

# collected a node-based resources: make the resourceline for SGE script
# only a #$ -l by itself causes a problem
resourceline = ""
if len(resources): resourceline = "\n#$ -l "+",".join(resources)

# postg16 of formchk  only when %chk is encountered
postg16=""
pat = re.compile(r'%chk=(.*)', re.IGNORECASE)
ss = re.findall(pat,comfile)
if len(ss):
    chkptfile = ss.pop()
    try:    
        chkptroot = chkptfile[0:chkptfile.rindex('.chk')]
    except: 
        chkptroot = chkptfile
    postg16 = f"formchk {chkptroot}.chk chkptroot.{fchk}"

# CREATION OF THE SCRIPT FILE
#scriptfile = "script." + str(os.getpid()) # unique filename (not for now?)
scriptfile = job+".sge"

scriptform = lambda : """#!/bin/csh

#$ -S /bin/csh
#$ -N {jobname}s
#$ -cwd
#$ -q {queue}s.q 
#$ -pe threaded {ppn}d {resourceline}s
#$ -o {job}s.o$JOB_ID
#$ -j y
#$ -cwd

# use the SGE provided TMPDIR which will be cleaned up automatically
setenv GAUSS_SCRDIR ${TMPDIR}
setenv g16root {g16root}s
source {g16root}s/g16/bsd/g16.login

echo job running on `hostname` with GAUSS_SCRDIR as $GAUSS_SCRDIR

{g16root}s/g16/g16 {fname}s

{postg16}s
"""

# "w" opens for writing, with truncate if the file exists (i.e., overwrites)
with open(scriptfile,'w') as f:
    f.write(scriptform())

if execute:
    os.system(f"qsub {scriptfile}")
    os.system(f"rm -f {scriptfile}")

