
package org.workflowsim.examples.scheduling;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.FileNotFoundException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator; 
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.power.PowerDatacenter;
import org.cloudbus.cloudsim.power.PowerDatacenterBroker;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.power.PowerHostUtilizationHistory;
import org.cloudbus.cloudsim.power.PowerVm;
import org.cloudbus.cloudsim.power.models.PowerModel;
import org.cloudbus.cloudsim.power.models.PowerModelCubic;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.workflowsim.ClusterStorage;
import org.workflowsim.Job;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

/**
 * The FCFS Scheduling Algorithm with power aware stuff
 */
public class FCFSPower {

    /**
     * Creates main() to run this example This example has only one
     * datacenter and one storage.
     */
    public static void main(String[] args) {

        String logFileName = "log";


        // First step: Initialize the WorkflowSim package.
        // ============================================================
        {
            // Use FCFS
            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.FCFS;

            // No planning because FCFS is a dynamic scheduling algorithm
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;

            // ??ds
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.LOCAL;

            // No overheads
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);;

            // No Clustering
            ClusteringParameters cp = new ClusteringParameters
                (0, 0, ClusteringParameters.ClusteringMethod.NONE, null);

            // The exact number of vms may not necessarily be vmNum. If
            // the data center or the host doesn't have sufficient
            // resources the exact vmNum would be smaller than that.
            // Take care. ??ds there's no need for this to be in a
            // static object, only used here!
            int vmNum = 50;

            // Initialize static Parameters object
            Parameters.init(vmNum, "config/dax/Montage_100.xml", null,
                            null, op, cp, sch_method, pln_method,
                            null, 0);
            ReplicaCatalog.init(file_system);
        }


        // Initialize the CloudSim library
        // ============================================================
        {
            // number of grid users
            int num_user = 1;

            // should we write a CloudSim trace file?
            boolean trace_flag = false;

            CloudSim.init(num_user, Calendar.getInstance(), trace_flag);
        }


        // Initialise the rest
        // ============================================================
        WorkflowEngine wfEngine = null;
        {
            // Create a datacenter that can handle workflows
            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");

            // Create a WorkflowPlanner with one scheduler
            WorkflowPlanner wfPlanner = null;
            try {
                wfPlanner = new WorkflowPlanner("planner_0", 1);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }

            // Create a WorkflowEngine.
            wfEngine = wfPlanner.getWorkflowEngine();

            // Create a list of VMs. The userId of a vm is basically the id
            // of the scheduler that controls this vm.
            List<Vm> vmlist0 =
                createVMs(wfEngine.getSchedulerId(0), Parameters.getVmNum());

            // Submits this list of vms to this WorkflowEngine.
            wfEngine.submitVmList(vmlist0, 0);

            // Binds the data centers with the scheduler.
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);
        }


        // Simulate!
        // ============================================================
        {
            // Divert the simultation logs to a file
            OutputStream logFile = null;
            try {
                logFile = new FileOutputStream(new File("a"));
            } catch(FileNotFoundException e) {
                throw new RuntimeException(e);
            }
            Log.setOutput(logFile);

            CloudSim.startSimulation();

            List<Job> outputList0 = wfEngine.getJobsReceivedList();

            CloudSim.stopSimulation();

            // But print summary to screen
            Log.setOutput(System.out);
            printJobList(outputList0);
        }

    }

    /** 
     * Function to set up a data center
     */
    protected static WorkflowDatacenter createDatacenter(String name) {

        // Create a list of machines (Hosts).
        List<Host> hostList = new ArrayList<Host>();
        for (int i = 0; i < 20; i++) {

            // Each Host contains one or more PEs (CPUs/Cores), create a
            // list to store these PEs. Each Pe stores Pe id and MIPS.
            List<Pe> peList = new ArrayList<Pe>();
            int mips = 2000;
            peList.add(new Pe(0, new PeProvisionerSimple(mips)));
            peList.add(new Pe(1, new PeProvisionerSimple(mips)));

            // Now create the host itself
            int ram = 2048; // host memory (MB)
            long storage = 1000000; // host storage
            int bandWidth = 10000;
            PowerModel powerModel = new PowerModelCubic(1.0, 0.3); 
            hostList.add
                (new PowerHost(i,
                               new RamProvisionerSimple(ram),
                               new BwProvisionerSimple(bandWidth),
                               storage,
                               peList,
                               new VmSchedulerTimeShared(peList),
                               powerModel));            
        }

        // Create a DatacenterCharacteristics object that stores the
        // properties of a data center.
        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double time_zone = 10.0;      // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;		// the cost of using memory in this resource
        double costPerStorage = 0.1;	// the cost of using storage in this resource
        double costPerBw = 0.1;			// the cost of using bw in this resource
        WorkflowDatacenter datacenter = null;
        DatacenterCharacteristics characteristics 
            = new DatacenterCharacteristics
            (arch, os, vmm, hostList, time_zone, cost, costPerMem,
             costPerStorage, costPerBw);


        // The bandwidth within a data center in MB/s. the number comes
        // from the futuregrid site, you
        // can specify your bw.
        int maxTransferRate = 15;

        // Create a single storage object in a list
        LinkedList<Storage> storageList = new LinkedList<Storage>();
        HarddriveStorage s1 = new HarddriveStorage(name, 1e12);
        s1.setMaxTransferRate(maxTransferRate);
        storageList.add(s1); 

        // ??ds
        VmAllocationPolicy vmAllocationPolicy
            = new VmAllocationPolicySimple(hostList);

        // Make the data center itself at last!
        try {
            datacenter = new WorkflowDatacenter
                (name, characteristics, vmAllocationPolicy, storageList, 0);

        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        return datacenter;
    }



    /**
     * Create a list of VMs ready for use
     */
    protected static List<Vm> createVMs(int userId, int vms) {

        // Set VM Parameters
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
        int mips = 1000;
        long bw = 1000;
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name

        // Create the specified number of VMs
        List<Vm> list = new LinkedList<Vm>();
        for(int i = 0; i < vms; i++) {
            list.add
                (new PowerVm
                 (i, userId, mips, pesNumber, ram, bw, size, 1, vmm,
                  new CloudletSchedulerSpaceShared(),
                  300));
        }

        return list;
    }


    /** 
     * Print the results of the jobs.
     */
    protected static void printJobList(List<Job> list) {

        // Create sorted copy of list
        Comparator<Job> comp = new Comparator<Job>() {
            @Override
            public int compare(Job o1, Job o2) {
                return Integer.compare(o1.getCloudletId(), o2.getCloudletId());
            }
        };
        List<Job> sorted = new ArrayList<Job>(list);
        Collections.sort(sorted, comp);

        // String to format output lines
        String formatString = "%12s%12s%12s%12s%12s%12s%12s%12s%n";
        
        // Header
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.format(formatString,
                   "STATUS",
                   "Cloudlet ID",
                   "DC ID" ,
                   "VM ID",
                   "CPU Time",
                   "Start Time",
                   "Finish Time",
                   "Depth"
                   );

        // Class to format floats to strings
        DecimalFormat dft = new DecimalFormat("###.##");

        
        for (Job job : sorted) {

            String status = null;
            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                status = "SUCCESS";
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                status = "FAILED";
            }

            Log.format(formatString,
                       status,
                       job.getCloudletId(),
                       job.getResourceId(),
                       job.getVmId(),
                       dft.format(job.getActualCPUTime()),
                       dft.format(job.getExecStartTime()),
                       dft.format(job.getFinishTime()),
                       job.getDepth()
                       );
        }

    }
}
