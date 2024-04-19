#################################################
#  manypoints.py . Script to submit points to a cluster
#   
#   Daniel Egana-Ugrinovic / Matthew Low
#    Stony Brook University
#    Institute for Advanced Study
#    August 2017
#   
#   
#################################################
# Directories summary:
# homedir: where the mg5 tar bundle and process tar bundle is located
# localworkdir: the directory from where the manypoints.py script is run, to prepare the grid and send the jobs to the nodes. 
# localworkdir/onepointdir: directory where the grid of onepoint.py, condor execute.sh and excecute.jdl  files are written
# nodedir: the node work directory
# nodedir/mgver: where MG5 is installed on the node.
# nodedir/mgver/bin: where the process directory must go. also onepoint.py needs to be copied there.
################################################
# Program summary:
# 1. Generates a grid of scripts onepoint.py, execute.sh and execute.jdl 
# 2. The script execute.sh contains all the commands to be sent to the node. It 
#    installs MG5 on the node and sends and runs one script onepoint.py, which runs madgraph on the node.
# 
################################################


import os

mgver = 'MG5_aMC_v2_6_0'

homedir = '/export/scratch2/danielegana/'
mgtarname = mgver+'bundle.tgz'
mgtar = homedir+mgtarname
mgdir = homedir+mgver
process = 'alignedLHC3j'
processtar = homedir+process+'.tgz'
localworkdir = os.getcwd()

MFcs  = [50,51]
yLs   = [0.5]
DMs  = [2]
Mphis = [110]

##################################################################
# FLATTEN SCAN PARAMETERS
par1 = []
par2 = []
par3 = []
par4 = []
for yL in yLs:
  for DM in DMs:
    for MFc in MFcs:
      for Mphi in Mphis:
        par1.append(yL)
        par2.append(DM)
        par3.append(MFc)
        par4.append(Mphi)

# SETUP RESULTS
if not os.path.isdir('onepointdir/'):
  os.makedirs('onepointdir') 

##################################################################
#
#      WRITE DOWN one_point.py, condor file to execute and condor jdl
#
##################################################################
for a in range(0,len(par1)):

  ##################################################################
  # set parameters
  pid = 9000007
  [DM,MFc,Mphi]      = [par2[a],par3[a],par4[a]]
  pointname = 'yL_'+str(yL)+'_DM_'+str(DM)+'_MFc_'+str(MFc)+'_Mphi_'+str(Mphi)

  ##################################################################
  # 1. WRITE onepoint.py
  ##################################################################

  file = open('onepoint.py','r')
  lines = []
  for line in file:
    split = line.split()        
    if 'yLs =' in line:
      split[2] = ('%9.4f' % (yL))
      lines.append((' '.join(split))+'\n')

    elif 'DMs =' in line:
      split[2] = ('%9.4f' % (DM))
      lines.append((' '.join(split))+'\n')
      
    elif 'MFcs =' in line:
      split[2] = ('%9.4f' % (MFc))
      lines.append((' '.join(split))+'\n')
      
    elif 'Mphis =' in line:
      split[2] = ('%9.4f' % (Mphi))
      lines.append((' '.join(split))+'\n')
      
    else:
      lines.append(line)
  file.close()
  
  file = open('new_one_point.py','w')
  for line in lines:
    file.write(line)
  file.close()
  
  os.system('mv new_one_point.py '+localworkdir+'/onepointdir/'+pointname+'.py')

  ##################################################################
  # 2. WRITE EXECUTABLE TO BE SENT TO NODE
  ##################################################################


  execfilename = 'exec_'+pointname+'.sh'
  executefile = open(localworkdir+'/onepointdir/'+execfilename,'w')
  executefile.write('#!/bin/bash\n')

  #set env variables to compile pythia/delphes correctly
  executefile.write('export VO_CMS_SW_DIR=/cvmfs/atlas.cern.ch \n')
  executefile.write('export COIN_FULL_INDIRECT_RENDERING=1\n')
  executefile.write('export ROOTSYS=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/root/6.10.04-x86_64-slc6-gcc62-opt\n')
  executefile.write('export PATH=$ROOTSYS/bin:/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/Gcc/gcc620_x86_64_slc6/6.2.0/x86_64-slc6/bin:$PATH \n')
  executefile.write('export LD_LIBRARY_PATH=$ROOTSYS/lib:/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/tbb/44_20160413-x86_64-slc6-gcc62-opt/lib:/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/Gcc/gcc620_x86_64_slc6/6.2.0/x86_64-slc6/lib64:/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/Gcc/gcc620_x86_64_slc6/6.2.0/x86_64-slc6/lib:${LD_LIBRARY_PATH} \n')
  executefile.write('export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:$ROOTSYS/lib \n')
  executefile.write('export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase \n')
  executefile.write("alias setupATLAS='source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh' \n")

  #copy madgraph to node.
  nodedir = '$_CONDOR_SCRATCH_DIR'
  #nodedir = '/export/scratch2/danielegana/mg5try'
  
  # untar madgraph
  executefile.write('cp -r '+mgtar+' '+nodedir+'\n')
  executefile.write('cd '+nodedir+'\n')
  executefile.write('tar xvfz '+mgtarname+'\n')
  executefile.write('cd '+nodedir+'\n')

  #install boost, hepmc, pythia8, lhapdf6 and madanalysis5
  executefile.write('cd '+nodedir+'/'+mgver+' \n')
  executefile.write(nodedir+'/'+mgver+'/HEPTools/HEPToolsInstallers/HEPToolInstaller.py boost --boost_tarball=./HEPTools/boost_1_59_0.tar.gz \n')
  executefile.write(nodedir+'/'+mgver+'/HEPTools/HEPToolsInstallers/HEPToolInstaller.py zlib --zlib_tarball=./HEPTools/zlib-1.2.10.tar.gz \n')
  executefile.write(nodedir+'/'+mgver+'/HEPTools/HEPToolsInstallers/HEPToolInstaller.py hepmc --hepmc_tarball=./HEPTools/hepmc2.06.09.tgz \n')
  # The following 3 lines were an attempt to install pythia from a tarball, but then madgraph doesn't work properly. For this reason, we install pythia from the mg5 interface downloading it.
  #  executefile.write(nodedir+'/'+mgver+'/HEPTools/HEPToolsInstallers/HEPToolInstaller.py pythia8 --pythia8_tarball=./HEPTools/pythia82.tgz \n')
  #  executefile.write(nodedir+'/'+mgver+'/HEPTools/HEPToolsInstallers/HEPToolInstaller.py lhapdf6 --lhapdf6_tarball=./HEPTools/LHAPDF-6.1.6.tar.gz \n')
  #  executefile.write(nodedir+'/'+mgver+'/HEPTools/HEPToolsInstallers/HEPToolInstaller.py mg5amc_py8_interface --mg5amc_py8_interface_tarball=./HEPTools/MG5aMC_PY8_interface.tar.gz \n')
  executefile.write('cp '+localworkdir+'/installpythia8 '+nodedir+'/'+mgver+'/bin/ \n')
  executefile.write('cd '+nodedir+'/'+mgver+'/bin \n')
  executefile.write(nodedir+'/'+mgver+'/bin/mg5_aMC installpythia8 \n')
  # Madanalysis is not needed, we don't install it.
  #  executefile.write(nodedir+'/'+mgver+'/HEPTools/HEPToolsInstallers/HEPToolInstaller.py madanalysis5 --madanalysis5_tarball=./HEPTools/ma5.tgz \n')

  # Install Delphes
  executefile.write('cd '+nodedir+'/'+mgver+' \n')
  executefile.write('tar xvfz Delphes.tgz \n')
  executefile.write('cd '+nodedir+'/'+mgver+'/Delphes \n')
  executefile.write('./configure && make \n')

  # Copy proc_card and generate process
  executefile.write('cp '+localworkdir+'/'+process+'card.dat '+nodedir+'/'+mgver+'/bin/ \n')
  executefile.write('cd '+nodedir+'/'+mgver+'/bin \n')
  executefile.write(nodedir+'/'+mgver+'/bin/mg5_aMC '+process+'card.dat \n')

  # Copy and run one script onepoint.py to the node. This starts the Madgraph simulation and generates the lhco files.
  executefile.write('cp '+localworkdir+'/onepointdir/'+pointname+'.py '+nodedir+'/'+mgver+'/bin/'+process+'/ \n')

  executefile.write('cd '+nodedir+'/'+mgver+'/bin/'+process+' \n')
 # executefile.write('python '+pointname+'.py \n')
  
  executefile.close()
   
  os.system('chmod u+x '+localworkdir+'/onepointdir/'+execfilename)

  ##################################################################
  ## 3. WRITE EXECUTABLE TO BE SENT TO NODE
  ##################################################################
  
  jdlfilename = "exec_"+pointname+".jdl.base"
  jdlfile = open(localworkdir+'/onepointdir/'+jdlfilename,'w')
  jdlfile.write('Universe = vanilla \n')
  jdlfile.write('Notification = Always \n')
  jdlfile.write('Executable = '+localworkdir+'/onepointdir/'+execfilename+'\n')
  jdlfile.write('Arguments = $(Process) $(Cluster) \n')
  jdlfile.write('GetEnv = False \n')
  jdlfile.write('Output = /export/scratch2/danielegana/condorlog/output.$(Cluster).$(Process) \n')
  jdlfile.write('Error = /export/scratch2/danielegana/condorlog/err.$(Cluster).$(Process) \n')
  jdlfile.write('Log = /export/scratch2/danielegana/condorlog/log.$(Cluster).$(Process) \n')
  jdlfile.write('should_transfer_files = YES \n')
  jdlfile.write('when_to_transfer_output = ON_EXIT \n')
  jdlfile.write('next_job_start_delay=1 \n')
  jdlfile.write('Queue 1 \n')
  jdlfile.close()

  ##################################################################
  ## 4. SUBMIT TO CLUSTER
  ##################################################################
  os.system('condor_submit '+localworkdir+'/onepointdir/'+jdlfilename+' \n')
#  os.system('sh '+localworkdir+'/onepointdir/'+execfilename)
