'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 04/17/2013
'
'*********************************************************************************************************

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesResultsCleanup
	Inherits clsAnalysisResources

#Region "Methods"

	''' <summary>
	''' Obtains resources necessary for performing analysis results cleanup
	''' </summary>
	''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
	''' <remarks>No resources needed for performing results transfer. Function merely meets inheritance requirements</remarks>
	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

#End Region

End Class
