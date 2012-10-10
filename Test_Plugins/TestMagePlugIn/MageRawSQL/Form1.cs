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

namespace MageRawSQL {
    public delegate void ActionCommand(string mode);

    public partial class Form1 : Form {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        ProcessingPipeline mPipeline = null;

        public Form1() {
            InitializeComponent();

            traceLog.Info("Starting");

            // wire me up to receive commands from other GUI components
            rawSQLPanel1.OnAction += DoCommand;
            statusPanel1.OnAction += DoCommand;
            sqLiteOutputPanel1.OnAction += DoCommand;
            rawSprocPanel1.OnAction += DoCommand;

            // tell pipeline processor where to look for loadable module DLLs
            FileInfo fi = new FileInfo(System.Windows.Forms.Application.ExecutablePath);
            ModuleDiscovery.ExternalModuleFolder = fi.DirectoryName;
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
                case "show_query_in_list_display":
                    mPipeline = MakeQueryDisplayPipeline();
                    break;
                case "show_sproc_in_list_display":
                    mPipeline = MakeSprocDisplayPipeline();
                    break;
                case "output_all_to_sqlite_db":
                    mPipeline = MakeSQLiteOutputPipeline();
                    break;
                case "output_selected_to_sqlite_db":
                    MessageBox.Show("This feature is not implemented yet");
                    //                    mPipeline = MakeSQLiteOutputPipeline("Output", LVPipelineSource.Modes.Selected);
                    break;
                default:
                    return;
            }
            mPipeline.OnStatusMessageUpdated += HandleStatusMessageUpdated;
            mPipeline.Run();
        }
        #region Functions that build pipelines

        private ProcessingPipeline MakeSprocDisplayPipeline() {
            ProcessingPipeline pipeline = new ProcessingPipeline("SprocDisplayPipeline");

            // make source module in pipeline to read jobs from DMS as sproc query
            // and connect it to display
            MSSQLReader mod = new MSSQLReader();
            mod.server = rawSprocPanel1.ServerName;
            mod.database = rawSprocPanel1.DatabaseName;
            mod.sprocName = rawSprocPanel1.SprocName;
            foreach (KeyValuePair<string, string> arg in rawSprocPanel1.SprocArgs) {
                mod.AddParm(arg.Key, arg.Value);
            }
            pipeline.RootModule = pipeline.AddModule("Reader", mod);
            ListDisplayControl.ConnectToPipeline(pipeline, "Reader", listDisplayControl1, 100);

            return pipeline;
        }


        private ProcessingPipeline MakeQueryDisplayPipeline() {
            string readerModuleType = "MSSQLReader";
            if (rawSQLPanel1.ServerName.ToLower() == "sqlite") {
                readerModuleType = "SQLiteReader";
            }
            ProcessingPipeline pipeline = new ProcessingPipeline("QueryDisplayPipeline");

            // make source module in pipeline to read jobs from DMS as database query
            // and connect it to display
            pipeline.RootModule = pipeline.MakeModule("Reader", readerModuleType);
            ListDisplayControl.ConnectToPipeline(pipeline, "Reader", listDisplayControl1, 100);

            // set module parameters and return pipeline
            pipeline.SetModuleParameters("Reader", GetModuleParamsForReader());
            return pipeline;
        }


        private ProcessingPipeline MakeSQLiteOutputPipeline() {
            ProcessingPipeline pipeline = new ProcessingPipeline("SQLiteOutputPipeline");

            // make source module in pipeline to read jobs from DMS as database query
            // and connect it to display
            pipeline.RootModule = pipeline.MakeModule("Reader", "MSSQLReader");
            ListDisplayControl.ConnectToPipeline(pipeline, "Reader", listDisplayControl1, 100);

            pipeline.MakeModule("Writer", "SQLiteWriter");
            pipeline.ConnectModules("Reader", "Writer");

            // set module parameters and return pipeline
            pipeline.SetModuleParameters("Reader", GetModuleParamsForReader());
            pipeline.SetModuleParameters("Writer", GetModuleParamsForSQLite());
            return pipeline;
        }

        #endregion

        #region Helper functions for building pipelines

        private List<KeyValuePair<string, string>> GetModuleParamsForReader() {
            List<KeyValuePair<string, string>> moduleParams = new List<KeyValuePair<string, string>>();
            moduleParams.Add(new KeyValuePair<string, string>("server", rawSQLPanel1.ServerName));
            moduleParams.Add(new KeyValuePair<string, string>("database", rawSQLPanel1.DatabaseName));
            moduleParams.Add(new KeyValuePair<string, string>("sqlText", rawSQLPanel1.SQL));
            return moduleParams;
        }

        private List<KeyValuePair<string, string>> GetModuleParamsForSQLite() {
            List<KeyValuePair<string, string>> moduleParams = new List<KeyValuePair<string, string>>();
            moduleParams.Add(new KeyValuePair<string, string>("DbPath", sqLiteOutputPanel1.DatabaseName));
            moduleParams.Add(new KeyValuePair<string, string>("TableName", sqLiteOutputPanel1.TableName));
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
