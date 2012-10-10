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

namespace MageFileCopy {
    public delegate void ActionCommand(string mode);

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
            fileCopyPanel1.OnAction += DoCommand;
            jobListPanel1.OnAction += DoCommand;
            jobIDListPanel1.OnAction += DoCommand;
            jobDataPackagePanel1.OnAction += DoCommand;
            fileSelectionPanel1.OnAction += DoCommand;
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
                case "get_jobs":
                    mPipeline = MakeJobQueryPipeline();
                    break;
                case "get_selected_files":
                    mPipeline = MakeFileListPipeline(LVPipelineSource.Modes.Selected);
                    break;
                case "get_all_files":
                    mPipeline = MakeFileListPipeline(LVPipelineSource.Modes.All);
                    break;
                case "copy_selected_files":
                    mPipeline = MakeFileCopyPipeline(LVPipelineSource.Modes.Selected);
                    break;
                default:
                    return;
            }
            mPipeline.OnStatusMessageUpdated += HandleStatusMessageUpdated;
            mPipeline.Run();
        }

        #region Functions that build pipelines

        private ProcessingPipeline MakeFileCopyPipeline(LVPipelineSource.Modes mode) {
            ProcessingPipeline pipeline = new ProcessingPipeline("FileCopyPipeline");

            // make client module as a data source for a list view control and add it to pipeline
            LVPipelineSource source = new LVPipelineSource(listDisplayControl2, mode);
            pipeline.RootModule = pipeline.AddModule("Selected Files", source);

            // create file copy module and connect it to the data source
            pipeline.MakeModule("File Copier", "FileCopy");
            pipeline.ConnectModules("Selected Files", "File Copier");

            // create a file writer to build manifest
            pipeline.MakeModule("Manifest File Writer", "DelimitedFileWriter");
            pipeline.ConnectModules("File Copier", "Manifest File Writer");

            // set parameters and return pipeline
            pipeline.SetModuleParameters("File Copier", GetModuleParamsForFileCopier());
            pipeline.SetModuleParameters("Manifest File Writer", GetModuleParamsForManifiestFileWriter());
            return pipeline;
        }

        private ProcessingPipeline MakeJobQueryPipeline() {
            ProcessingPipeline pipeline = new ProcessingPipeline("JobQueryPipeline");

            // make source module in pipeline to read jobs from DMS as database query
            // and connect it to display
            pipeline.RootModule = pipeline.MakeModule("Reader", "MSSQLReader");
            ListDisplayControl.ConnectToPipeline(pipeline, "Reader", listDisplayControl1, 100);

            // set module parameters and return pipeline
            pipeline.SetModuleParameters("Reader", GetModuleParamsForReader());
            return pipeline;
        }

        private ProcessingPipeline MakeFileListPipeline(LVPipelineSource.Modes mode) {
            ProcessingPipeline pipeline = new ProcessingPipeline("FileListPipeline");

            // make client module as a data source for a list view control and add it to pipeline
            LVPipelineSource source = new LVPipelineSource(listDisplayControl1, mode);
            pipeline.RootModule = pipeline.AddModule("Selected Items", source);

            // create file filter module and connect it to the data source and to the display
            pipeline.MakeModule("File Filter", "FileListFilter");
            pipeline.ConnectModules("Selected Items", "File Filter");
            ListDisplayControl.ConnectToPipeline(pipeline, "File Filter", listDisplayControl2, 100);

            // set parameters and return pipeline
            pipeline.SetModuleParameters("File Filter", GetModuleParamsForFileFilter());
            return pipeline;
        }

        #endregion

        #region Helper functions for building pipelines

        private List<KeyValuePair<string, string>> GetModuleParamsForReader() {
            string sourceType = tabControl1.SelectedTab.Tag.ToString();
            string sql = "";
            switch (sourceType) {
                case "Query":
                    sql = jobListPanel1.SQL;
                    break;
                case "Job_ID_List":
                    sql = jobIDListPanel1.SQL;
                    break;
                case "Data_Package":
                    sql = jobDataPackagePanel1.SQL;
                    break;
            }
            List<KeyValuePair<string, string>> moduleParams = new List<KeyValuePair<string, string>>();
            moduleParams.Add(new KeyValuePair<string, string>("server", "gigasax"));
            moduleParams.Add(new KeyValuePair<string, string>("database", "DMS5"));
            moduleParams.Add(new KeyValuePair<string, string>("sqlText", sql));
            return moduleParams;
        }

        private List<KeyValuePair<string, string>> GetModuleParamsForFileFilter() {
            List<KeyValuePair<string, string>> moduleParams = new List<KeyValuePair<string, string>>();
            moduleParams.Add(new KeyValuePair<string, string>("SourceFolderColumnName", "Folder"));
            moduleParams.Add(new KeyValuePair<string, string>("IDColumnName", "Job"));
            moduleParams.Add(new KeyValuePair<string, string>("OutputColumnList", "File|+|text, Folder, Job, Dataset, Dataset_ID, Tool, Settings_File, Parameter_File, Instrument"));
            foreach (string selector in fileSelectionPanel1.FileSelectors) {
                moduleParams.Add(new KeyValuePair<string, string>("FileNameSelector", selector.Trim()));
            }
            return moduleParams;
        }

        private List<KeyValuePair<string, string>> GetModuleParamsForFileCopier() {
            List<KeyValuePair<string, string>> moduleParams = new List<KeyValuePair<string, string>>();
            moduleParams.Add(new KeyValuePair<string, string>("OutputColumnList", "File, Job, Dataset, Dataset_ID, Tool, Settings_File, Parameter_File, Instrument"));
            moduleParams.Add(new KeyValuePair<string, string>("OutputFolderPath", fileCopyPanel1.OutputFolder));
            moduleParams.Add(new KeyValuePair<string, string>("OutputMode", "Prefix"));
            moduleParams.Add(new KeyValuePair<string, string>("IDColumnName", "Job"));
            return moduleParams;
        }

        private List<KeyValuePair<string, string>> GetModuleParamsForManifiestFileWriter() {
            List<KeyValuePair<string, string>> moduleParams = new List<KeyValuePair<string, string>>();
            string fileName = string.Format("Manifest_{0:yyMMddhhmmss}.txt", System.DateTime.Now);
            moduleParams.Add(new KeyValuePair<string, string>("FilePath", Path.Combine(fileCopyPanel1.OutputFolder, fileName)));
            return moduleParams;
        }

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
