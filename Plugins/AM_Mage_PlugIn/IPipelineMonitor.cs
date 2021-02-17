namespace AnalysisManager_Mage_PlugIn
{
    internal interface IPipelineMonitor
    {
        void ConnectPipelineQueueToStatusHandlers(Mage.PipelineQueue pipelineQueue);
        void ConnectPipelineToStatusHandlers(Mage.ProcessingPipeline pipeline);
    }
}
