// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "Keep for reference", Scope = "member", Target = "~M:AnalysisManagerMSGFDBPlugIn.MSGFPlusUtils.ParseMSGFDBModifications(System.String,System.Text.StringBuilder,System.Int32,System.Collections.Generic.IReadOnlyCollection{System.String},System.Collections.Generic.IReadOnlyCollection{System.String},System.Collections.Generic.IReadOnlyCollection{System.String})~System.Boolean")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Silently ignore a file deletion error in this case", Scope = "member", Target = "~M:AnalysisManagerMSGFDBPlugIn.MSGFPlusUtils.ConvertMZIDToTSV(System.String,System.String,System.String)~System.String")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Silently ignore a file deletion error in this case", Scope = "member", Target = "~M:AnalysisManagerMSGFDBPlugIn.MSGFPlusUtils.ConvertMZIDToTSV(System.String,System.String,System.String,System.String)~System.String")]
[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Silently ignore a file deletion error in this case", Scope = "member", Target = "~M:AnalysisManagerMSGFDBPlugIn.CreateMSGFDBSuffixArrayFiles.DeleteLockFile(System.IO.FileSystemInfo)")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:AnalysisManagerMSGFDBPlugIn.AnalysisToolRunnerMSGFDB.RunMSGFPlus(System.String,System.IO.FileInfo@,System.Boolean@,System.Boolean@)~AnalysisManagerBase.CloseOutType")]
[assembly: SuppressMessage("Readability", "RCS1123:Add parentheses when necessary.", Justification = "Parentheses not needed", Scope = "member", Target = "~M:AnalysisManagerMSGFDBPlugIn.MSGFPlusUtils.InitializeFastaFile(System.String,System.String,System.Single@,System.Boolean@,System.String@,System.String,System.Int32)~AnalysisManagerBase.CloseOutType")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMSGFDBPlugIn.AnalysisResourcesMSGFDB")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMSGFDBPlugIn.AnalysisToolRunnerMSGFDB")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMSGFDBPlugIn.CreateMSGFDBSuffixArrayFiles")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMSGFDBPlugIn.FastaContaminantUtility")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMSGFDBPlugIn.ScanTypeFileCreator")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManagerMSGFDBPlugIn.MSGFPlusUtils.ParseMSGFPlusParameterFile(System.Boolean,System.String,System.String,System.String,System.String,System.Collections.Generic.Dictionary{System.String,System.String},System.IO.FileInfo@,System.IO.FileInfo@)~AnalysisManagerBase.CloseOutType")]
