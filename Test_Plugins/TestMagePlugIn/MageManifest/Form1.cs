using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Mage;
using log4net;
using System.IO;
using MageClientLib;

namespace MageManifest {

    public partial class Form1 : Form {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        ProcessingPipeline mPipeline = null;

        public Form1() {
            InitializeComponent();

            // kick the logger into action
            traceLog.Info("Starting");

            // tell pipeline processor where to look for loadable module DLLs
            FileInfo fi = new FileInfo(System.Windows.Forms.Application.ExecutablePath);
            ModuleDiscovery.ExternalModuleFolder = fi.DirectoryName;

            // wire me up to receive commands from other GUI components
            metadataExportPanel1.OnAction += DoCommand;
            inputFilePanel1.OnAction += DoCommand;
            statusPanel1.OnAction += DoCommand;

        }

        // execute a command by building and running 
        // the appropriate pipeline (or cancelling
        // the current pipeline activity)
        private void DoCommand(string command) {

            // cancel the currently running pipeline
            if (command == "cancel_operation" && mPipeline != null && mPipeline.Running) {
                mPipeline.Cancel();
                return;
            }
            // don't allow another pipeline if one is currently running
            if (mPipeline != null && mPipeline.Running) {
                MessageBox.Show("Pipeline is already active");
                return;
            }
            // build and run the pipeline appropriate to the command
            switch (command) {
                case "read_file_content":
                    mPipeline = MakeFileContentReadPipeline();
                    break;
                case "save_metadata":
                    string sourceType = tabControl1.SelectedTab.Tag.ToString();
                    mPipeline = MakeDatasetFactorsPipeline(sourceType);
                    break;
                default:
                    return;
            }
            mPipeline.OnStatusMessageUpdated += HandleStatusMessageUpdated;
            mPipeline.Run();
        }


        #region Functions that build pipelines

        private ProcessingPipeline MakeDatasetFactorsPipeline(string mode) {
            ProcessingPipeline pipeline = new ProcessingPipeline("DatasetFactorsPipeline");

            // make client module as a data source and add it to pipeline
            // either for a list view control or a text display control
            IBaseModule source = null;
            if (mode == "TextDisplaySource") {
                source = new TDPipelineSource(textDisplayControl1);
            } else
                if (mode == "LVDisplaySource") {
                    LVPipelineSource.Modes md = LVPipelineSource.Modes.All;
                    if (listDisplayControl1.List.SelectedItems.Count > 0) {
                        md = LVPipelineSource.Modes.Selected;
                    }
                    source = new LVPipelineSource(listDisplayControl1, md);
                }
            pipeline.RootModule = pipeline.AddModule("Selected Files", source);

            pipeline.MakeModule("Factors", "DatasetFactors");
            pipeline.ConnectModules("Selected Files", "Factors");
            //
            pipeline.SetModuleParameter("Factors", "Server", "gigasax");
            pipeline.SetModuleParameter("Factors", "Database", "DMS5");
            pipeline.SetModuleParameter("Factors", "OutputFilePath", metadataExportPanel1.OutputFilePath);
            pipeline.SetModuleParameter("Factors", "OutputTableName", metadataExportPanel1.OutputTableName);
            pipeline.SetModuleParameter("Factors", "OutputFormat", metadataExportPanel1.OutputFormat);
            pipeline.SetModuleParameter("Factors", "OutputType", metadataExportPanel1.OutputType);

            return pipeline;
        }

        private ProcessingPipeline MakeFileContentReadPipeline() {
            ProcessingPipeline pipeline = new ProcessingPipeline("FileContentReadPipeline");

            // make module as a data source to read contents of manifest file and add it to pipeline
            pipeline.RootModule = pipeline.MakeModule("Reader", "DelimitedFileReader");

            ListDisplayControl.ConnectToPipeline(pipeline, "Reader", listDisplayControl1, 100);

            // set parameters and return pipeline
            pipeline.SetModuleParameter("Reader", "FilePath", inputFilePanel1.FilePath);

            return pipeline;
        }


        #endregion


        #region Helper functions for building pipelines
        #endregion

        #region Functions for Handling Status Messages

        private void HandleStatusMessageUpdated(string Message) {
            // handle the status update messages from the currently running pipeline
            Console.WriteLine(Message); // FUTURE: logging?

            // the current pipleline will call this function from its own thread
            // we need to do the cross-thread thing to update the GUI
            StatusMessageUpdated ncb = SetStatusMessage;
            Invoke(ncb, new object[] { Message });
        }
        private void SetStatusMessage(string Message) {
            // this is targeted by the cross-thread invoke from HandleStatusMessageUpdated
            statusPanel1.SetStatusMessage(Message);
        }

        #endregion


    }
}
