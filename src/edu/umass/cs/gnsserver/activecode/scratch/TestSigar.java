package edu.umass.cs.gnsserver.activecode.scratch;

/**
 * Display all process information.
 */
public class TestSigar {
/*
extends SigarCommandBase {

    private boolean isSingleProcess;

    public ProcInfo(Shell shell) {
        super(shell);
    }

    public ProcInfo() {
        super();
    }

    protected boolean validateArgs(String[] args) {
        return true;
    }

    public String getUsageShort() {
        return "Display all process info";
    }

    public boolean isPidCompleter() {
        return true;
    }

    public void output(String[] args) throws SigarException {
        this.isSingleProcess = false;

        if ((args.length != 0) && args[0].startsWith("-s")) {
            this.isSingleProcess = true;
        }

        if (this.isSingleProcess) {
            for (int i=1; i<args.length; i++) {
                try {
                    output(args[i]);
                } catch (SigarException e) {
                    println("(" + e.getMessage() + ")");
                }
                println("\n------------------------\n");
            }
        }
        else {
            long[] pids = this.shell.findPids(args);

            for (int i=0; i<pids.length; i++) {
                try {
                    output(String.valueOf(pids[i]));
                } catch (SigarPermissionDeniedException e) {
                    println(this.shell.getUserDeniedMessage(pids[i]));
                } catch (SigarException e) {
                    println("(" + e.getMessage() + ")");
                }
                println("\n------------------------\n");
            }
        }
    }

    public void output(String pid) throws SigarException {
        println("pid=" + pid);
        try {
            println("state=" + sigar.getProcState(pid));
        } catch (SigarException e) {
            if (this.isSingleProcess) {
                println(e.getMessage());
            }
        }
        try {
            println("mem=" + sigar.getProcMem(pid));
        } catch (SigarException e) {}
        try {
            println("cpu=" + sigar.getProcCpu(pid));
        } catch (SigarException e) {}
        try {
            println("cred=" + sigar.getProcCred(pid));
        } catch (SigarException e) {}
        try {
            println("credname=" + sigar.getProcCredName(pid));
        } catch (SigarException e) {}
        try {
        	println("diskio=" + sigar.getProcDiskIO(pid));
        } catch (SigarException e) {}

 	try {
                println("cumulative diskio=" + sigar.getProcCumulativeDiskIO(pid));
        } catch (SigarException e) {}

    }

    public static void main(String[] args) throws Exception {
        new ProcInfo().processCommand(args);
    }
    */
}