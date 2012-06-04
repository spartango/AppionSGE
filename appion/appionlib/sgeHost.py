import processingHost
import subprocess

class SGEHost(processingHost.ProcessingHost):
    def __init__ (self, configDict=None):
        processingHost.ProcessingHost.__init__(self)  #initialize parent
        self.type="SGE"
        self.execCommand="qsub"
        self.statusCommand="qstat"
        self.scriptPrefix="#$"
        if configDict:
            self.configure(configDict)
            
    ##generateHeaders (jobObject)
    #Takes a job object or no arguments. If jobObject is supplied it uses it to 
    #construct processing host specific resource directives.  If no argument is
    #supplied used the currentJob property set in the class instance.        
    # SGE example
    # qsub -l mem_free=1G -cwd -r y -V -e name.err -o name.out -N frln_rec scrip.csh
    def generateHeaders(self, jobObject=None):
        if jobObject != None:
            currentJob=jobObject
        elif self.currentJob != None:
            currentJob=self.currentJob
        else:
            raise UnboundLocalError ("Current Job not set")
        #Every Shell Script starts by indicating shell type
        header = "#!" + self.getShell() + "\n"
        header += " -S " + self.getShell() + "\n"
        #add job attribute headers
        # TODO
        # if currentJob.getWalltime():
        #     header += self.scriptPrefix +" -l walltime=" + str(currentJob.getWalltime())+":00:00\n"
        
        if currentJob.getNodes():
            header += self.scriptPrefix +" -pe ompi " + str(currentJob.getNodes()) + "\n"
        #     if currentJob.getPPN():
        #         header += ":ppn=" + str(currentJob.getPPN())
        #     header += "\n"
        
        # if currentJob.getCpuTime():
        #     header += self.scriptPrefix +" -l cput=" + str(currentJob.getCpuTime()) + ":00:00\n"
            
        if currentJob.getMem():
            header += self.scriptPrefix +" -l mem_free=" + str(currentJob.getMem()) + 'gb\n'
        
        # Specify the current directory as the working directory
        header += self.scriptPrefix + " -cwd \n"

        # Specify that we want the verbosity high
        header += self.scriptPrefix + " -V \n"

        # if currentJob.getPmem():
        #     header += self.scriptPrefix +" -l pmem=" + str(currentJob.getPmem()) + "mb\n"
            
        # if currentJob.getQueue():
        #     header += self.scriptPrefix +" -q " + currentJob.getQueue() + "\n"
            
        # if currentJob.getAccount():
        #     header += self.scriptPrefix +" -A " + currentJob.getAccount()+ "\n"
            
        #Add any custom headers for this processing host.
        for line in self.getAdditionalHeaders():
            header += self.scriptPrefix + " " + line + "\n"            
        #add some white space     
        if self.preExecLines:    
            header += "\n\n"
        #Add any custom line that should be added to jobfile (Ex. module purge)
        for line in self.getPreExecutionLines():
            header += line + "\n"
        #add some white space  
        header += "\n\n"
        return header
    
    #translateOutput (outputString)
    #Takes the outputSring returned by executing a command (executeCommand()) and
    #Translates it into a Job ID which can be used to check job status. 
    # Example
    #Your job 5151 ("test.sh") has been submitted

    def translateOutput (self, outputString):
        outputList = outputString.split(' ')
        try:
            jobID= int(outputList[2])
        except Exception:
            return False
        return jobID      
    
    # qstat output example
    #     job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID 
    # ----------------------------------------------------------------------------------------------------------------- 
    #  5149 0.55500 test.sh    agupta       r     06/04/2012 13:04:52 default@baltar.mcb.harvard.edu     1      
    
    def checkJobStatus(self, procHostJobId):
        statusCommand = self.getStatusCommand()
        
        try:
            process = subprocess.Popen(statusCommand, 
                                        stdout=subprocess.PIPE, 
                                        stderr=subprocess.PIPE, 
                                        shell=True)
            returnCode =process.wait()
            # TODO
            if returnCode != 0:
                #return unknown status if check resulted in a error
                returnStatus = 'U'
            else:
                rstring = process.communicate()[0]
                try:
                    # Grab the 3rd line (with this job status) and the 5th column
                    status =  rstring.split('\n')[2].split()[4]
                    #translate SGE status codes to appion codes
                    if status == 'c' or status == 'e':
                        #Job completed of is exiting
                        returnStatus = 'D'
                      
                    elif status == 'r':
                        #Job is running
                        returnStatus = 'R'
                    elif status == 'qw':
                        returnStatus = 'Q'
                    else:
                        #Interpret everything else as queued
                        returnStatus = 'Q'
                except IndexError:
                    # HACK
                    # The job line isn't there. We'll assume this is because it's done
                    returnStatus = 'D'

        except Exception:
            # Failed to initialize process (call to qstat)
            returnStatus = 'U'

        return returnStatus
    