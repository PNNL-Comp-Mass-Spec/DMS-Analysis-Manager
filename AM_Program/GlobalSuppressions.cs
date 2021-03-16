
// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Acceptable design pattern", Scope = "module")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parenthese are not required", Scope = "member", Target = "~M:AnalysisManagerProg.MainProcess.PauseManagerForCooldown(System.Int32)")]
[assembly: SuppressMessage("Readability", "RCS1192:Unnecessary usage of verbatim string literal.", Justification = "@ included for readability", Scope = "member", Target = "~M:AnalysisManagerProg.CodeTest.TestProgRunnerIDPicker")]
[assembly: SuppressMessage("Simplification", "RCS1190:Join string expressions.", Justification = "Separate for readability", Scope = "member", Target = "~M:AnalysisManagerProg.CodeTest.RunMSConvert")]
[assembly: SuppressMessage("Simplification", "RCS1190:Join string expressions.", Justification = "Separate for readability", Scope = "member", Target = "~M:AnalysisManagerProg.CodeTest.TestProgRunnerIDPicker")]
[assembly: SuppressMessage("Style", "IDE0059:Unnecessary assignment of a value", Justification = "Used for debugging", Scope = "member", Target = "~M:AnalysisManagerProg.CodeTest.ProcessDtaRefineryLogFiles(System.Int32,System.Int32)~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE0059:Unnecessary assignment of a value", Justification = "Used for debugging", Scope = "member", Target = "~M:AnalysisManagerProg.CodeTest.TestMALDIDataUnzip(System.String)~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE0059:Unnecessary assignment of a value", Justification = "Used for debugging", Scope = "member", Target = "~M:AnalysisManagerProg.CodeTest.TestProteinDBExport(System.String)~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE0059:Unnecessary assignment of a value", Justification = "Used for debugging", Scope = "member", Target = "~M:AnalysisManagerProg.CodeTest.TestRunSP")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerProg.AnalysisMgrSettings")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerProg.CleanupMgrErrors")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerProg.CleanupMgrErrors.CleanupActionCodes")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerProg.CleanupMgrErrors.CleanupModes")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerProg.CodeTestAM")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerProg.MainProcess")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerProg.PluginLoader")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "module")]