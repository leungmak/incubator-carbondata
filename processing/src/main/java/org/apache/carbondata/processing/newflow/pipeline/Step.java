package org.apache.carbondata.processing.newflow.pipeline;

/**
 * Represent one step in the pipeline.
 */
public interface Step {
  /**
   * all steps should implement this interface to do the corresponding action
   *
   * @param context context for loading, it should be used for saving output of this step
   * @return result indicating success or fail
   */
  void doWork(PipelineContext context);

  /**
   * Factory interface to create the Step.
   */
  public interface Factory {
    Step create();
  }

}
