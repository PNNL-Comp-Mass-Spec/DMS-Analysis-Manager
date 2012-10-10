using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Reflection;

// module that creates and runs a Mage pipeline for one or more input files 
//
// it expects to receive path information for files via its standard tabular input
// (its FileContentProcessor base class provides the basic functionality)
//
// this module builds a filtering sub-pipeline to process each file 
// and runs that in the the same thread the module is currently running in
//
// there are two internally-defined file processing sub-pipelines, that have a delimited file
// reader module that reads rows from a file and passed them to a filter module, which passes
// its rows to a writer module.
//
// one of the internally-defined sub-pipelines uses a delimited file writer module, 
// and the other uses a SQLite writer.
//
// to use either of these internally-defined sub-pipelines, the client need only 
// supply the name of the filter module to be used (by setting the FileFilterModuleName property)
// if the DatabaseName and TableName properties are set, the SQLite database sub-pipeline will be used
// otherwise the delimited file writer sub-pipeline is used
//
// the sub-pipeline can also be supplied by the client by setting the FileProcessingPipelineGenerator
// delegate to call the client's pipeline generator function

namespace Mage {

    // delegate for a client-supplied function that this module can call to build its sub-pipeline
    public delegate ProcessingPipeline FileProcessingPipelineGenerator(string inputFilePath, string outputFilePath);

    public class FileSubPipelineBroker : FileContentProcessor {

        #region Member Variables

        // handle to the currently running sub-pipeline
        private ProcessingPipeline mPipeline = null;

        // delegate that this module calls to build sub-pipeline
        private FileProcessingPipelineGenerator ProcessingPipelineMaker = null;

        #endregion

        #region Functions Available to Clients

        // define a delegate function that will be called by this module 
        // to construct and run a file processing pipeline 
        // for each file handled by this broker module
        public void SetPipelineMaker(FileProcessingPipelineGenerator maker) {
            ProcessingPipelineMaker = maker;
        }

        #endregion

        #region Properties

        // name of filter module that is used when internally defined sub-pipeline is used
        public string FileFilterModuleName { get; set; }
        public Dictionary<string, string> FileFilterParameters { get; set; }

        // parameters for SQLite database writer for internally defined sub-pipelines using SQLite Writer
        public string DatabaseName { get; set; }
        public string TableName { get; set; }

        #endregion

        #region Constructors

        public FileSubPipelineBroker() {
            // set up to use our own default sub-pipeline maker 
            //in case the client doesn't give us another one
            FileFilterModuleName = ""; // client must set this property to use internally defined sub-pipelines
            DatabaseName = ""; // client must set these properties to user internally-defined SQLiteWriter sub-pipeline
            TableName = "";
        }

        #endregion

        #region Overrides

        public override void Prepare() {
            base.Prepare();

            if (FileFilterModuleName != "") {
                // optionally, set up our sub-pipeline generator delegate to use
                // an internally-defined sub-pipeline, according to module settings
                if (DatabaseName != "") {
                    ProcessingPipelineMaker = new FileProcessingPipelineGenerator(MakeDefaultSQLiteProcessingPipeline);
                } else {
                    ProcessingPipelineMaker = new FileProcessingPipelineGenerator(MakeDefaultFileProcessingPipeline);
                }
                // set up to use the file renaming function provided by the filter module
                SetUpFileRenamer(FileFilterModuleName);
            }
        }

        // this is called from the base class for each input file to be processed
        protected override void ProcessFile(string sourceFile, string sourcePath, string destPath, ref bool stop) {
            if (ProcessingPipelineMaker != null) {
                mPipeline = ProcessingPipelineMaker(sourcePath, destPath);
                mPipeline.OnStatusMessageUpdated += UpdateStatus;
                mPipeline.RunRoot(null); // we are already in a pipeline thread - don't run sub-pipeline in a new one

                // sub-pipeline encountered fatal error, interrupt the main pipeline
                if (mPipeline.CompletionCode != "") {
                    throw new Exception(mPipeline.CompletionCode);
                }
            }
        }

        #endregion

        #region Internally-Defined Processing Pipelines

        // this function builds the a file processing pipeline 
        // which is a simple file filtering process that has a file reader module, file filter module, and file writer module,
        // using a filter module specified by the FilterModuleClasssName, which must be set by the client
        private ProcessingPipeline MakeDefaultFileProcessingPipeline(string inputFilePath, string outputFilePath) {
            ProcessingPipeline pipeline = new ProcessingPipeline("DefaultFileProcessingPipeline");
            string sourceModule = "Reader";
            string filterModule = "Filter";
            string writerModule = "Writer";

            if (FileFilterModuleName != "") {
                // make source module in pipeline to read contents of file
                pipeline.RootModule = pipeline.MakeModule(sourceModule, "DelimitedFileReader");
                //
                pipeline.SetModuleParameter(sourceModule, "FilePath", inputFilePath);

                // make filter module and wire to source module
                pipeline.MakeModule(filterModule, FileFilterModuleName);
                if (FileFilterParameters != null) {
                    pipeline.SetModuleParameters(filterModule, new List<KeyValuePair<string, string>>(FileFilterParameters));
                }
                pipeline.ConnectModules(sourceModule, filterModule);

                // make sink module and connect to filter
                pipeline.MakeModule(writerModule, "DelimitedFileWriter");
                //
                pipeline.ConnectModules(filterModule, writerModule);
                pipeline.SetModuleParameter(writerModule, "FilePath", outputFilePath);
            }
            return pipeline;
        }


        // this function builds a file processing pipeline
        // which is a simple file filtering process that has a file reader module, file filter module, and SQLiteWriter writer module,
        // using a filter module specified by the FilterModuleClasssName, which must be set by the client
        private ProcessingPipeline MakeDefaultSQLiteProcessingPipeline(string inputFilePath, string outputFilePath) {
            ProcessingPipeline pipeline = new ProcessingPipeline("FileProcessingSubPipeline");
            string sourceModule = "Reader";
            string filterModule = "Filter";
            string writerModule = "Writer";

            if (FileFilterModuleName != "") {
                // make source module in pipeline to read contents of file
                pipeline.RootModule = pipeline.MakeModule(sourceModule, "DelimitedFileReader");
                //
                pipeline.SetModuleParameter(sourceModule, "FilePath", inputFilePath);

                // make filter module and wire to source module
                pipeline.MakeModule(filterModule, FileFilterModuleName);
                if (FileFilterParameters != null) {
                    pipeline.SetModuleParameters(filterModule, new List<KeyValuePair<string, string>>(FileFilterParameters));
                }
                pipeline.ConnectModules(sourceModule, filterModule);

                // make sink module and connect to filter
                pipeline.MakeModule(writerModule, "SQLiteWriter");
                pipeline.ConnectModules(filterModule, writerModule);
                //
                pipeline.SetModuleParameter(writerModule, "DbPath", DatabaseName);
                pipeline.SetModuleParameter(writerModule, "TableName", TableName);
            }
            return pipeline;
        }

        #endregion

        // wire the filter module's file renaming method to this broker module's delegate
        // if such a renaming method is present
        protected void SetUpFileRenamer(string filterModule) {
            ProcessingPipeline pipeline = new ProcessingPipeline("FileProcessingSubPipeline");
            pipeline.MakeModule(filterModule, FileFilterModuleName);
            BaseModule bm = (BaseModule)pipeline.GetModule(filterModule);
            MethodInfo mi = bm.GetType().GetMethod("RenameOutputFile");
            if (mi != null) {
                ContentFilter filterMod = (ContentFilter)bm;
                SetOutputFileNamer(new OutputFileNamer(filterMod.RenameOutputFile));
            }
        }
    }
}