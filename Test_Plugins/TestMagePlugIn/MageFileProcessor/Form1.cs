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
using log4net;
using MageClientLib;
using System.Reflection;

namespace MageFileProcessor {

    public partial class Form1 : Form {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        ProcessingPipeline mPipeline = null;

        public Form1() {
            InitializeComponent();

            // kick the logger into action
            traceLog.Info("Starting");

            // tell modules where to look for loadable module DLLs
            FileInfo fi = new FileInfo(System.Windows.Forms.Application.ExecutablePath);
            ModuleDiscovery.ExternalModuleFolder = fi.DirectoryName;

            // set up filter selection list
            ModuleDiscovery.SetupFilters();
            fileProcessingPanel1.SetupFilters();

            // wire me up to receive commands from other GUI components
            fileProcessingPanel1.OnAction += DoCommand;
            folderDestinationPanel1.OnAction += DoCommand;
            jobDataPackagePanel1.OnAction += DoCommand;
            jobFilePanel1.OnAction += DoCommand;
            jobIDListPanel1.OnAction += DoCommand;
            jobListPanel1.OnAction += DoCommand;
            localFolderPanel1.OnAction += DoCommand;
            sqLiteDestinationPanel1.OnAction += DoCommand;
            statusPanel1.OnAction += DoCommand;

            // labels for display list control panels
            JobListDisplayControl.PageTitle = "Jobs";
            FileListDisplayControl.PageTitle = "Files";
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
            try {
                // build and run the pipeline appropriate to the command
                switch (command) {
                    case "get_jobs":
                        mPipeline = MakeJobQueryPipeline();
                        break;
                    case "process_local_folder":
                        mPipeline = MakePipelineToGetLocalFileList();
                        break;
                    case "get_selected_job_files":
                        mPipeline = MakeFileListPipeline(LVPipelineSource.Modes.Selected);
                        break;
                    case "get_all_job_files":
                        mPipeline = MakeFileListPipeline(LVPipelineSource.Modes.All);
                        break;
                    case "copy_selected_files":
                        mPipeline = MakeFileCopyPipeline(LVPipelineSource.Modes.Selected);
                        break;
                    case "copy_all_files":
                        mPipeline = MakeFileCopyPipeline(LVPipelineSource.Modes.All);
                        break;
                    case "process_selected_files":
                        mPipeline = MakePipelineToFilterSelectedfiles(LVPipelineSource.Modes.Selected);
                        break;
                    case "process_all_files":
                        mPipeline = MakePipelineToFilterSelectedfiles(LVPipelineSource.Modes.All);
                        break;

                    default:
                        return;
                }
                mPipeline.OnStatusMessageUpdated += HandleStatusMessageUpdated;
                mPipeline.OnRunCompleted += HandlePipelineCompletion;
                mPipeline.Run();
            } catch (Exception e) {
                MessageBox.Show(e.Message);
            }
        }

        #region Functions that build pipelines

        // pipeline to copy files that are selected in the files list display to a local folder
        private ProcessingPipeline MakeFileCopyPipeline(LVPipelineSource.Modes mode) {
            ProcessingPipeline pipeline = new ProcessingPipeline("FileCopyPipeline");
            string sourceModule = "Selected Files";
            string copierModule = "File Copier";
            string reportModule = "Manifest File Writer";

            // make client module as a data source for a list view control and add it to pipeline
            LVPipelineSource source = new LVPipelineSource(FileListDisplayControl, mode);
            pipeline.RootModule = pipeline.AddModule(sourceModule, source);

            // create file copy module and connect it to the data source
            pipeline.MakeModule(copierModule, "FileCopy");
            pipeline.ConnectModules(sourceModule, copierModule);

            // create a file writer to build manifest
            pipeline.MakeModule(reportModule, "DelimitedFileWriter");
            pipeline.ConnectModules(copierModule, reportModule);

            // set parameters and return pipeline
            pipeline.SetModuleParameter(copierModule, "OutputColumnList", "File, Job, Dataset, Dataset_ID, Tool, Settings_File, Parameter_File, Instrument");
            pipeline.SetModuleParameter(copierModule, "OutputFolderPath", folderDestinationPanel1.OutputFolder);
            pipeline.SetModuleParameter(copierModule, "OutputMode", "Prefix");
            pipeline.SetModuleParameter(copierModule, "IDColumnName", "Job");

            string manifestFileName = string.Format("Manifest_{0:yyMMddhhmmss}.txt", System.DateTime.Now);
            pipeline.SetModuleParameter(reportModule, "FilePath", Path.Combine(folderDestinationPanel1.OutputFolder, manifestFileName));

            return pipeline;
        }

        // pipeline to get list of files from results folders of jobs that are selected in list display
        // and deliver the list to the files list display
        private ProcessingPipeline MakeFileListPipeline(LVPipelineSource.Modes mode) {
            ProcessingPipeline pipeline = new ProcessingPipeline("FileListPipeline");
            string sourceModule = "Selected Jobs";
            string filterModule = "File Filter";

            // make client module as a data source for a list view control and add it to pipeline
            LVPipelineSource source = new LVPipelineSource(JobListDisplayControl, mode);
            pipeline.RootModule = pipeline.AddModule(sourceModule, source);

            // create file filter module and connect it to the data source and to the display
            pipeline.MakeModule(filterModule, "FileListFilter");
            pipeline.ConnectModules(sourceModule, filterModule);
            ListDisplayControl.ConnectToPipeline("Files", pipeline, filterModule, FileListDisplayControl, 100);

            // set parameters and return pipeline
            pipeline.SetModuleParameter(filterModule, "SourceFolderColumnName", "Folder");
            pipeline.SetModuleParameter(filterModule, "IDColumnName", "Job");
            pipeline.SetModuleParameter(filterModule, "OutputColumnList", "File|+|text, Folder, Job, Dataset, Dataset_ID, Tool, Settings_File, Parameter_File, Instrument");
            foreach (string selector in jobFilePanel1.FileSelectors) {
                pipeline.SetModuleParameter(filterModule, "FileNameSelector", selector.Trim());
            }
            return pipeline;
        }

        // pipeline to get selected list of files from local folder into list display
        private ProcessingPipeline MakePipelineToGetLocalFileList() {
            ProcessingPipeline pipeline = new ProcessingPipeline("PipelineToGetLocalFileList");
            string sourceModule = "Reader";

            // make source module in pipeline to get list of files in local directory
            pipeline.RootModule = pipeline.MakeModule(sourceModule, "FileListFilter");

            // wire it up to the display list
            ListDisplayControl.ConnectToPipeline("Files", pipeline, sourceModule, FileListDisplayControl, 100);

            // set module parameters and return pipeline
            pipeline.SetModuleParameter(sourceModule, "FolderPath", localFolderPanel1.Folder);
            pipeline.SetModuleParameter(sourceModule, "FileNameSelector", localFolderPanel1.FileNameFilter);
            return pipeline;
        }

        // pipeline to get list of jobs from DMS into list display
        private ProcessingPipeline MakeJobQueryPipeline() {
            ProcessingPipeline pipeline = new ProcessingPipeline("JobQueryPipeline");
            string sourceModule = "Reader";

            // make source module in pipeline to read jobs from DMS as database query
            // and connect it to display
            pipeline.RootModule = pipeline.MakeModule(sourceModule, "MSSQLReader");
            ListDisplayControl.ConnectToPipeline("Jobs", pipeline, sourceModule, JobListDisplayControl, 100);

            // set module parameters and return pipeline
            string sourceType = JoblistSourceTabs.SelectedTab.Tag.ToString();
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
            pipeline.SetModuleParameter(sourceModule, "server", "gigasax");
            pipeline.SetModuleParameter(sourceModule, "database", "DMS5");
            pipeline.SetModuleParameter(sourceModule, "sqlText", sql);
            return pipeline;
        }

        // pipeline to filter contents of files that are selected in the files list display
        private ProcessingPipeline MakePipelineToFilterSelectedfiles(LVPipelineSource.Modes mode) {
            ProcessingPipeline pipeline = new ProcessingPipeline("PipelineToFilterSelectedfiles");
            string sourceModule = "Selected Files";
            string brokerModule = "Sub-Pipeline Broker";
            string reportModule = "Report File Writer";

            // make client module as a data source for a list view control and add it to pipeline
            LVPipelineSource source = new LVPipelineSource(FileListDisplayControl, mode);
            pipeline.RootModule = pipeline.AddModule(sourceModule, source);

            // make file sub-pipeline processing broker to run a filter pipeline against files from list
            FileSubPipelineBroker subPipelineBroker = new FileSubPipelineBroker();
            //
            string outputFolderPath = folderDestinationPanel1.OutputFolder;
            subPipelineBroker.OutputFolderPath = outputFolderPath;
            string filterName = fileProcessingPanel1.SelectedFilterClassName;
            subPipelineBroker.FileFilterModuleName = filterName;
            subPipelineBroker.FileFilterParameters = fileProcessingPanel1.GetParameters();
            //
            if (FilterOutputTabs.SelectedTab.Tag.ToString() == "SQLite_Output") {
                subPipelineBroker.DatabaseName = sqLiteDestinationPanel1.DatabaseName;
                subPipelineBroker.TableName = sqLiteDestinationPanel1.TableName;
                outputFolderPath = Path.GetDirectoryName(sqLiteDestinationPanel1.DatabaseName);
            }
            pipeline.AddModule(brokerModule, subPipelineBroker);
            pipeline.ConnectModules(sourceModule, brokerModule);

            // create a file writer to build manifest
            pipeline.MakeModule(reportModule, "DelimitedFileWriter");
            pipeline.ConnectModules(brokerModule, reportModule);

            string flt = filterName.Replace(" ", "_");
            string reportFileName = string.Format("Runlog_{0}_{1:yyMMdd-hhmmss}.txt", flt, System.DateTime.Now);
            pipeline.SetModuleParameter(reportModule, "FilePath", Path.Combine(outputFolderPath, reportFileName));

            return pipeline;
        }

        #endregion

        #region Functions for Handling Status Messages

        // handle the status update messages from the currently running pipeline
        private void HandleStatusMessageUpdated(string Message) {
            // the current pipleline will call this function from its own thread
            // we need to do the cross-thread thing to update the GUI
            StatusMessageUpdated ncb = SetStatusMessage;
            Invoke(ncb, new object[] { Message });
        }

        // handle the status completion message from the currently running pipeline
        private void HandlePipelineCompletion(string Message) {

            // pipeline didn't blow up, make nice reassuring message
            if (Message == "") Message = "Process completed normally";

            // the current pipleline will call this function from its own thread
            // we need to do the cross-thread thing to update the GUI
            StatusMessageUpdated ncb = SetStatusMessage;
            Invoke(ncb, new object[] { Message });
        }

        // this is targeted by the cross-thread invoke from HandleStatusMessageUpdated
        private void SetStatusMessage(string Message) {
            statusPanel1.SetStatusMessage(Message);
        }

        #endregion

    }
}
