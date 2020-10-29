// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerMSGFDBPlugIn.clsAnalysisResourcesMSGFDB")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "<Pending>", Scope = "type", Target = "~T:AnalysisManagerMSGFDBPlugIn.clsAnalysisToolRunnerMSGFDB")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Silently ignore a file deletion error in this case", Scope = "member", Target = "~M:AnalysisManagerMSGFDBPlugIn.MSGFPlusUtils.ConvertMZIDToTSV(System.String,System.String,System.String)~System.String")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Silently ignore a file deletion error in this case", Scope = "member", Target = "~M:AnalysisManagerMSGFDBPlugIn.MSGFPlusUtils.ConvertMZIDToTSV(System.String,System.String,System.String,System.String)~System.String")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManagerMSGFDBPlugIn.MSGFPlusUtils.ParseMSGFPlusParameterFile(System.Boolean,System.String,System.String,System.String,System.String,System.Collections.Generic.Dictionary{System.String,System.String},System.IO.FileInfo@,System.IO.FileInfo@)~AnalysisManagerBase.CloseOutType")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:AnalysisManagerMSGFDBPlugIn.clsAnalysisToolRunnerMSGFDB.RunMSGFPlus(System.String,System.IO.FileInfo@,System.Boolean@,System.Boolean@)~AnalysisManagerBase.CloseOutType")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:AnalysisManagerMSGFDBPlugIn.MSGFPlusUtils.InitializeFastaFile(System.String,System.String,System.Single@,System.Boolean@,System.String@,System.String,System.Int32)~AnalysisManagerBase.CloseOutType")]
