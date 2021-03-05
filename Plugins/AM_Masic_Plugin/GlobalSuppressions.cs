// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Design", "RCS1075:Avoid empty catch clause that catches System.Exception.", Justification = "Acceptable design pattern", Scope = "module")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMasicPlugin.AnalysisToolRunnerMASICBase")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Allowed", Scope = "type", Target = "~T:AnalysisManagerMasicPlugin.AnalysisToolRunnerMASICFinnigan")]
[assembly: SuppressMessage("Style", "IDE0066:Convert switch statement to expression", Justification = "Leave as-is for readability", Scope = "member", Target = "~M:AnalysisManagerMasicPlugin.AnalysisResourcesMASIC.GetResources~AnalysisManagerBase.CloseOutType")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManagerMasicPlugin.AnalysisResourcesMASIC")]
[assembly: SuppressMessage("Style", "IDE1006:Naming Styles", Justification = "Acceptable legacy name", Scope = "type", Target = "~T:AnalysisManagerMasicPlugin.AnalysisToolRunnerMASICAgilent")]
