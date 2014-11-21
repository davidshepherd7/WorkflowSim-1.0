
package org.workflowsim.examples.scheduling;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
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
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.power.PowerDatacenter;
import org.cloudbus.cloudsim.power.PowerDatacenterBroker;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.power.PowerHostUtilizationHistory;
import org.cloudbus.cloudsim.power.PowerVm;
import org.cloudbus.cloudsim.power.PowerVmAllocationPolicyMigrationAbstract;
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
            int vmNum = 5;

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

            // Create a list of VMs.The userId of a vm iss basically the
            // id of the scheduler that controls this vm.
            List<Vm> vmlist0 =
                createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum());

            // Submits this list of vms to this WorkflowEngine.
            wfEngine.submitVmList(vmlist0, 0);

            // Binds the data centers with the scheduler.
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);
        }


        // Simulate!
        // ============================================================
        {
            CloudSim.startSimulation();

            List<Job> outputList0 = wfEngine.getJobsReceivedList();

            CloudSim.stopSimulation();

            printJobList(outputList0);
        }

    }


    protected static WorkflowDatacenter createDatacenter(String name) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more
        //    Machines
        List<Host> hostList = new ArrayList<Host>();

        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.
        for (int i = 1; i <= 20; i++) {
            List<Pe> peList1 = new ArrayList<Pe>();
            int mips = 2000;
            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:
            peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
            peList1.add(new Pe(1, new PeProvisionerSimple(mips)));

            int hostId = 0;
            int ram = 2048; //host memory (MB)
            long storage = 1000000; //host storage
            int bw = 10000;
            PowerModel powerModel = new PowerModelCubic(1.0, 0.3);

            hostList.add
                (new PowerHost(hostId++,
                               new RamProvisionerSimple(ram),
                               new BwProvisionerSimple(bw),
                               storage,
                               peList1,
                               new VmSchedulerTimeShared(peList1),
                               powerModel));
        }

        // 5. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        String arch = "x86";      // system architecture
        String os = "Linux";          // operating system
        String vmm = "Xen";
        double time_zone = 10.0;         // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;		// the cost of using memory in this resource
        double costPerStorage = 0.1;	// the cost of using storage in this resource
        double costPerBw = 0.1;			// the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now
        WorkflowDatacenter datacenter = null;


        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                                                                                  arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


        // 6. Finally, we need to create a storage object.

	// The bandwidth within a data center in MB/s.
        int maxTransferRate = 15;// the number comes from the futuregrid site, you can specify your bw

        try {
            HarddriveStorage s1 = new HarddriveStorage(name, 1e12);
            s1.setMaxTransferRate(maxTransferRate);
            storageList.add(s1);
            datacenter = new WorkflowDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datacenter;
    }

    protected static List<Vm> createVM(int userId, int vms) {

        //Creates a container to store VMs. This list is passed to the broker later
        List<Vm> list = new LinkedList<Vm>();

        //VM Parameters
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
        int mips = 1000;
        long bw = 1000;
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name

        for(int i = 0; i < vms; i++) {
            list.add
                (new PowerVm
                 (i,
                  userId,
                  mips,
                  pesNumber,
                  ram,
                  bw,
                  size,
                  1,
                  vmm,
                  new CloudletSchedulerSpaceShared(),
                  300));
        }

        return list;
    }


    protected static void printJobList(List<Job> list) {
        int size = list.size();
        Job job;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
                      + "Data center ID" + indent + "VM ID" + indent + indent + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            job = list.get(i);
            Log.print(indent + job.getCloudletId() + indent + indent);

            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");

                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                              + indent + indent + indent + dft.format(job.getActualCPUTime())
                              + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                              + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("FAILED");

                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                              + indent + indent + indent + dft.format(job.getActualCPUTime())
                              + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                              + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            }
        }

    }
}
