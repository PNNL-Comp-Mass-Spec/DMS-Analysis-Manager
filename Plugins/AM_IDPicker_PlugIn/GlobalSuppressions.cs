// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "Keep for reference", Scope = "module")]
[assembly: SuppressMessage("CodeQuality", "IDE0079:Remove unnecessary suppression", Justification = "Ignore unreachable code", Scope = "member", Target = "~M:AnalysisManagerIDPickerPlugIn.AnalysisResourcesIDPicker.GetResources~AnalysisManagerBase.JobConfig.CloseOutType")]
[assembly: SuppressMessage("Redundancy", "RCS1213:Remove unused member declaration.", Justification = "Keep for reference", Scope = "module")]
[assembly: SuppressMessage("Simplification", "RCS1073:Convert 'if' to 'return' statement.", Justification = "Leave for debugging purposes", Scope = "member", Target = "~M:AnalysisManagerIDPickerPlugIn.AnalysisResourcesIDPicker.GetInputFiles(System.String,System.String,AnalysisManagerBase.JobConfig.CloseOutType@)~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE0028:Simplify collection initialization", Justification = "Uses the preferred syntax", Scope = "member", Target = "~M:AnalysisManagerIDPickerPlugIn.AnalysisToolRunnerIDPicker.RunTool~AnalysisManagerBase.JobConfig.CloseOutType")]
[assembly: SuppressMessage("Style", "IDE0028:Simplify collection initialization", Justification = "Uses the preferred syntax", Scope = "member", Target = "~M:AnalysisManagerIDPickerPlugIn.AnalysisToolRunnerIDPicker.StoreToolVersionInfo(System.String,System.Boolean)~System.Boolean")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerIDPickerPlugIn.AnalysisResourcesIDPicker")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerIDPickerPlugIn.AnalysisToolRunnerIDPicker")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:AnalysisManagerIDPickerPlugIn.AnalysisResourcesIDPicker.LookupLegacyFastaFileName~System.String")]
