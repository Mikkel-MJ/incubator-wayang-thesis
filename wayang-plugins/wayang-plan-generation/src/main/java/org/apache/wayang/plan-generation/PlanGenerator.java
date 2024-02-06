import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.basic.operators.UnarySource;
import org.apache.wayang.basic.operators.UnarySink;
import org.apache.wayang.basic.operators.Operator;

public class PlanGenerator {

  public static void main(String[] args) {
    // Create Wayang Plans
    // 1. Args should take some input such as number of plans to generate, and number of executionplans to generate
  }

  // Should be able to generate a WayangPlan that matches a workload
  public WayangPlan generatePlan(UnarySource source) {
    Operator currentOperator = source; 
    while (currentOperator.instanceOf(UnarySink.class)) {
      var nextOperator = generateNextOperator(currentOperator); 
      
      currentOperator.connectTo(0,nextOperator,0);
      currentOperator = nextOperator;
      
    }
    return null;
  }

  // should be able to give a operator back that can and make sense to connect to the currentOperator
  private Operator generateNextOperator(Operator currentOperator) {
    return null;
  }

  private float[][] markovTransistionMatrix;
}
