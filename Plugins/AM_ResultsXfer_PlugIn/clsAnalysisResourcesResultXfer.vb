'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 10/30/2008
'
' Last modified 10/30/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesResultXfer
	Inherits clsAnalysisResources

	'*********************************************************************************************************
	'Inherited class for obtaining resources necessary to perform analysis results transfer
	'*********************************************************************************************************

#Region "Constants"
#End Region

#Region "Module variables"
#End Region

#Region "Events"
#End Region

#Region "Properties"
#End Region

#Region "Methods"
	''' <summary>
	''' Obtains resources necessary for performing analysis results transfer
	''' </summary>
	''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
	''' <remarks>No resources needed for performing results transfer. Function merely meets inheritance requirements</remarks>
	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function
#End Region

End Class
