using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml;
using System.IO;
using System.Reflection;
using log4net;

// FUTURE: cancel, pause, resume
namespace Mage {

    public class ProcessingPipeline {

        private static readonly ILog traceLog = LogManager.GetLogger("TraceLog");

        public event StatusMessageUpdated OnStatusMessageUpdated;
        public event StatusMessageUpdated OnRunCompleted;

        #region Member Variables

        private Dictionary<string, IBaseModule> moduleList = new Dictionary<string, IBaseModule>();

        #endregion

        #region Properties

        public string CompletionCode { get; set;  }

        public IBaseModule RootModule { get; set; }

        public bool Running { get; set; }

        public string PipelineName { get; set; }

        #endregion

        #region Constructors

        public ProcessingPipeline(string name) {
            PipelineName = name;
            traceLog.Info(string.Format("Building pipeline '{0}'", PipelineName));
        }

        #endregion

        #region Private Functions


        #endregion

        #region Functions Available to Clients

        // invoke the run method on the root module of pipeline in separate thread from thread pool
        public void Run() {
            // fire off the pipeline
            ThreadPool.QueueUserWorkItem(RunRoot);
        }

        // call the Run method on the root module of pipeline (execution will be in caller's thread)
        public void RunRoot(Object state) {
            Running = true;
            CompletionCode = "";
            HandleStatusMessageUpdated("Running...");
            traceLog.Info(string.Format("Pipeline {0} started...", PipelineName));

            // give all modules in pipeline a chance to prepare themselves
            foreach (KeyValuePair<string, IBaseModule> modDef in moduleList) {
                modDef.Value.Prepare();
            }

            try {
                RootModule.Run(this);

                HandleStatusMessageUpdated("Process Complete");
                traceLog.Info(string.Format("Pipeline {0} completed...", PipelineName));

            } catch (Exception e) {
                CompletionCode = e.Message;
                HandleStatusMessageUpdated(e.Message);
                traceLog.Error(string.Format("Pipeline {0} failed: {1}...", PipelineName, e.Message));
            }

            // give all modules in pipeline a chance to clean up after themselves
            foreach (KeyValuePair<string, IBaseModule> modDef in moduleList) {
                modDef.Value.Cleanup();
            }

            Running = false;
            if (OnRunCompleted != null) {
                OnRunCompleted(CompletionCode);
            }
        }


        public void Cancel() {
            foreach (KeyValuePair<string, IBaseModule> modDef in moduleList) {
                modDef.Value.Cancel();
            }
        }

        public void SetModuleParameters(string moduleName, List<KeyValuePair<string, string>> moduleParams) {
            // get reference to module by name and send it the list of parameters
            IBaseModule mod = moduleList[moduleName];
            if (mod != null) {
                mod.SetParameters(moduleParams);
            } else {
                traceLog.Info(string.Format("Could not find module '{0}' to set parameters ({1})", moduleName, PipelineName));
            }
        }

        public void SetModuleParameter(string moduleName, string paramName, string paramValue) {
            // get reference to module by name, package the parameter, and send it to the module
            IBaseModule mod = moduleList[moduleName];
            if (mod != null) {
                List<KeyValuePair<string, string>> moduleParams = new List<KeyValuePair<string, string>>();
                moduleParams.Add(new KeyValuePair<string, string>(paramName, paramValue));
                mod.SetParameters(moduleParams);
            } else {
                traceLog.Info(string.Format("Could not find module '{0}' to set parameter '{1}'.", moduleName, paramName));
            }
        }

        public void ConnectModules(string upstreamModule, string downstreamModule) {
            // get reference to both the upstream and downstream modules by name
            // and wire the downstream module's pipeline event handlers to the upstream module's pipeline events

            try {
                IBaseModule modUp = moduleList[upstreamModule];
                IBaseModule modDn = moduleList[downstreamModule];
                modUp.ColumnDefAvailable += modDn.HandleColumnDef;
                modUp.DataRowAvailable += modDn.HandleDataRow;
                traceLog.Info(string.Format("Connected input of module '{0}' to output of module '{1} ({2})", downstreamModule, upstreamModule, PipelineName));
            } catch (Exception e) {
                string msg = string.Format("Failed to connect module '{0}' to module '{1} ({2}): {3}", downstreamModule, upstreamModule, PipelineName, e.Message);
                traceLog.Error(msg);
                throw new Exception(msg);
            }
        }

        public void ConnectExternalModule(string moduleName, ColumnDefHandler colHandler, DataRowHandler rowHandler) {
            IBaseModule mod = moduleList[moduleName];
            if (mod != null) {
                mod.ColumnDefAvailable += colHandler;
                mod.DataRowAvailable += rowHandler;
                traceLog.Info(string.Format("Connected external handler to module '{0}' ({1})", moduleName, PipelineName));
            } else {
                traceLog.Info(string.Format("Could not connect handler module to module '{0}' ({1})", moduleName, PipelineName));
            }
        }

        public IBaseModule MakeModule(string moduleName, string moduleType) {
            IBaseModule module = null;
            string ClassName = moduleType;
            Type modType = null;

            try {
                modType = ModuleDiscovery.GetModuleTypeFromClassName(ClassName);

                if (modType != null) {
                    module = (IBaseModule)Activator.CreateInstance(modType);
                } else {
                    throw new Exception("Class not found in searched assemblies");
                }
                return AddModule(moduleName, module);
            } catch (Exception e) {
                string msg = "Module '" + moduleName + ":" + moduleType + "' could not be created - " + e.Message;
                traceLog.Error(msg);
                throw new Exception(msg);
            }
        }


        public IBaseModule AddModule(string moduleName, IBaseModule module) {
            module.ModuleName = moduleName;
            moduleList.Add(moduleName, module);
            module.OnStatusMessageUpdated += HandleStatusMessageUpdated;
            traceLog.Info(string.Format("Added module '{0}' ({1})", moduleName, PipelineName));
            return module;
        }

        public IBaseModule GetModule(string moduleName) {
            return moduleList[moduleName];
        }

        #endregion

        #region Event Handlers

        private void HandleStatusMessageUpdated(string Message) {
            if (OnStatusMessageUpdated != null) {
                OnStatusMessageUpdated(Message);
            }
        }

        #endregion

        #region Build Pipeline From XML definitions

        public void Build(string pipelineSpec) {
            // step through XML module specification document
            // and build and wire modules as specified
            //
            string moduleName = "";
            string moduleType = "";
            string connectedTo = "";

            pipelineSpec = "<root>" + pipelineSpec + "</root>";
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(pipelineSpec);
            XmlNodeList xnl = doc.SelectNodes(".//module");

            // get next module description from specification
            foreach (XmlNode n in xnl) {
                moduleName = n.Attributes["name"].InnerText;
                moduleType = n.Attributes["type"].InnerText;
                // create the module
                IBaseModule mod = MakeModule(moduleName, moduleType);
                // wire it to an upstream module, if required
                XmlNode cn = n.Attributes["connectedTo"];
                if (cn != null) {
                    connectedTo = cn.InnerText;
                    ConnectModules(connectedTo, moduleName);
                } else {
                    // module with no upstream module 
                    // is assumed to be the root of the pipeline
                    // (we play by Highlander rules - there can be only one)
                    RootModule = mod;
                }
            }
        }

        public void SetAllModuleParameters(string pipelineModuleParams) {
            // step though XML document that defines parameters for modules,
            // and for each module in the document, extract a key/value list of paramters
            // and send them to the module

            // parse the XML definition of the module parameters
            pipelineModuleParams = "<root>" + pipelineModuleParams + "</root>";
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(pipelineModuleParams);

            // step through list of module sections in specification
            XmlNodeList xnl = doc.SelectNodes(".//module");
            // do section for current module in specifiction
            foreach (XmlNode modNode in xnl) {
                // get the name of the module that the paramters belong to
                string moduleName = modNode.Attributes["name"].InnerText;
                // build list of parameters for the module
                List<KeyValuePair<string, string>> moduleParams = new List<KeyValuePair<string, string>>();
                foreach (XmlNode parmNode in modNode.ChildNodes) {
                    string paramName = parmNode.Attributes["name"].InnerText;
                    string paramVal = parmNode.InnerText;
                    KeyValuePair<string, string> param = new KeyValuePair<string, string>(paramName, paramVal);
                    moduleParams.Add(param);
                }
                // send list of parameters to module
                SetModuleParameters(moduleName, moduleParams);
            }
        }

        #endregion

    }
}
