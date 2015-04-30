'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'*********************************************************************************************************

Option Strict On

''' <summary>
''' Interface for analysis resources
''' </summary>
''' <remarks></remarks>
Public Interface IAnalysisResources

#Region "Properties"
	ReadOnly Property Message() As String
#End Region

#Region "Methods"

	''' <summary>
	''' Initialize class
	''' </summary>
	''' <param name="mgrParams">Manager parameter object</param>
	''' <param name="jobParams">Job parameter object</param>
	''' <remarks></remarks>
    Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams)

	''' <summary>
	''' Initialize class
	''' </summary>
	''' <param name="mgrParams">Manager parameter object</param>
	''' <param name="jobParams">Job parameter object</param>
	''' <param name="statusTools">Status tools object</param>
	''' <remarks></remarks>
	Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal statusTools As IStatusFile)

	''' <summary>
	''' Main processing function for obtaining the required resources
	''' </summary>
	''' <returns>Status value indicating success or failure</returns>
	''' <remarks></remarks>
	Function GetResources() As IJobParams.CloseOutType

	''' <summary>
	''' Check the status of an analysis resource option
	''' </summary>
	''' <param name="resourceOption">Option to get</param>
	''' <returns>The option value (true or false)</returns>
	''' <remarks></remarks>
	Function GetOption(resourceOption As clsGlobal.eAnalysisResourceOptions) As Boolean

	''' <summary>
	''' Set the status of an analysis resource option
	''' </summary>
	''' <param name="resourceOption">Option to set</param>
	''' <param name="enabled">True or false</param>
	''' <remarks></remarks>
	Sub SetOption(resourceOption As clsGlobal.eAnalysisResourceOptions, enabled As Boolean)

#End Region

End Interface


