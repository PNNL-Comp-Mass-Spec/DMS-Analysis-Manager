using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.IO;
using Mage;
using MageClientLib;
using log4net;

namespace MageFileContents {

    public partial class Form1 : Form {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        ProcessingPipeline mPipeline = null;

        public Form1() {
            InitializeComponent();

            traceLog.Info("Starting");

            // wire me up to receive commands from other GUI components
            localFolderPanel1.OnAction += DoCommand;
            fileContentFilterPanel1.OnAction += DoCommand;
            statusPanel1.OnAction += DoCommand;

            // tell pipeline processor where to look for loadable module DLLs
            FileInfo fi = new FileInfo(System.Windows.Forms.Application.ExecutablePath);
            ModuleDiscovery.ExternalModuleFolder = fi.DirectoryName;

            // set up filter selection list
            fileContentFilterPanel1.FilterSelectionItems = "XCorr Example|XT2FHT";
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
                case "process_local_folder":
                    mPipeline = MakePipelineToGetLocalFileList();
                    break;
                case "filter_selected_files":
                    mPipeline = MakePipelineToFilterSelectedfiles(LVPipelineSource.Modes.Selected);
                    break;
                default:
                    return;
            }
            mPipeline.OnStatusMessageUpdated += HandleStatusMessageUpdated;
            mPipeline.Run();
        }

        #region Functions that build pipelines

        // pipeline to get selected list of files from folder specified
        // by parameters and load into display list
        private ProcessingPipeline MakePipelineToGetLocalFileList() {
            ProcessingPipeline pipeline = new ProcessingPipeline("PipelineToGetLocalFileList");

            // make source module in pipeline to get list of files in local directory
            pipeline.RootModule = pipeline.MakeModule("Reader", "FileListFilter");

            // wire it up to the display list
            ListDisplayControl.ConnectToPipeline(pipeline, "Reader", listDisplayControl1, 100);

            // set module parameters and return pipeline
            pipeline.SetModuleParameter("Reader", "FolderPath", localFolderPanel1.Folder);
            pipeline.SetModuleParameter("Reader", "FileNameSelector", localFolderPanel1.FileNameFilter);

            return pipeline;
        }

        private ProcessingPipeline MakePipelineToFilterSelectedfiles(LVPipelineSource.Modes mode) {
            ProcessingPipeline pipeline = new ProcessingPipeline("PipelineToFilterSelectedfiles");

            // make client module as a data source for a list view control and add it to pipeline
            LVPipelineSource source = new LVPipelineSource(listDisplayControl1, mode);
            pipeline.RootModule = pipeline.AddModule("Selected Files", source);

            // make file sub-pipeline processing broker 
            // to run a filter pipeline against files from list
            FileSubPipelineBroker mfb = new FileSubPipelineBroker();
            mfb.OutputFolderPath = fileContentFilterPanel1.OutputFolder;

            string filterName = fileContentFilterPanel1.FilterSelection;
            switch (filterName) {
                case "XCorr Example":
                    // let broker use its internal default sub-pipeline - just need to specify the filter module
                    mfb.FileFilterModuleName = "SequestFilter";
                    break;
                case "XT2FHT":
                    // give sub-pipeline processing broker a delegate to use to build the sub-pipeline
                    mfb.SetPipelineMaker(new FileProcessingPipelineGenerator(MakeXT2FHTFileProcessingPipeline));
                    // give sub-pipeline processing broker a delegate to use to rename the files that it processes
                    mfb.SetOutputFileNamer(new OutputFileNamer(RenameOutputFile));
                    break;
            }

            pipeline.AddModule("Broker", mfb);
            pipeline.ConnectModules("Selected Files", "Broker");

            // create a file writer to build manifest
            pipeline.MakeModule("Manifest File Writer", "DelimitedFileWriter");
            pipeline.ConnectModules("Broker", "Manifest File Writer");

            string fileName = string.Format("Manifest_{0:yyMMddhhmmss}.txt", System.DateTime.Now);
            pipeline.SetModuleParameter("Manifest File Writer", "FilePath", Path.Combine(fileContentFilterPanel1.OutputFolder, fileName));

            return pipeline;
        }

        #endregion

        #region Functions that build Sub-Pipelines for FileSubPipelineBroker modules
        // these functions will be supplied as delegates to file processing broker

        private ProcessingPipeline MakeXT2FHTFileProcessingPipeline(string inputFilePath, string outputFilePath) {
            ProcessingPipeline pipeline = new ProcessingPipeline("FileProcessingPipeline");

            // make source module in pipeline to read contents of file
            pipeline.RootModule = pipeline.MakeModule("Reader", "DelimitedFileReader");

            // make filter module and wire to source module
            pipeline.MakeModule("Filter", "XT2FHTFilter");
            pipeline.ConnectModules("Reader", "Filter");
            string colMap = "HitNum|+|text, ScanNum|Scan, ScanCount|+|text, ChargeState|Charge, MH|Peptide_MH, XCorr|+|text, DelCn|+|text, Sp|+|text, Reference|+|text, MultiProtein|Multiple_Protein_Count, Peptide|Peptide_Sequence, DelCn2|DeltaCn2, RankSp|+|text, RankXc|+|text, DelM|Delta_Mass, XcRatio|+|text, PassFilt|+|text, MScore|+|text, NumTrypticEnds|+|text";
            pipeline.SetModuleParameter("Filter", "OutputColumnList", colMap);

            // make sink module and connect to filter
            pipeline.MakeModule("Writer", "DelimitedFileWriter");
            pipeline.ConnectModules("Filter", "Writer");

            // set module parameters and return pipeline
            pipeline.SetModuleParameter("Reader", "FilePath", inputFilePath);
            pipeline.SetModuleParameter("Writer", "FilePath", outputFilePath);

            return pipeline;
        }

        // delegate that handles renaming of source file to output file for MakeXT2FHTFileProcessingPipeline
        private string RenameOutputFile(string sourceFile, Dictionary<string, int> fieldPos, object[] fields) {
            return sourceFile.Replace("_xt", "_fht");
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
