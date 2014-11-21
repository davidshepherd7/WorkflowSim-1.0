package org.workflowsim.power;

import java.util.List;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.workflowsim.WorkflowDatacenter;



/**
 * ??ds
 *
 * @author David Shepherd
 */
public class WorkflowPowerAwareDatacenter extends WorkflowDatacenter implements PowerAware {
    

    public WorkflowPowerAwareDatacenter
        (String name, DatacenterCharacteristics characteristics,
         VmAllocationPolicy vmAllocationPolicy,
         List<Storage> storageList, double schedulingInterval) throws Exception {
        super(name, characteristics, vmAllocationPolicy, 
              storageList, schedulingInterval);
    }

    public double getPower() {
        throw new UnsupportedOperationException();
    }

    
    
}
