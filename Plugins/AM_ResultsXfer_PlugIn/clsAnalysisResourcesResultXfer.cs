'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 10/30/2008
'
'*********************************************************************************************************

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesResultXfer
    Inherits clsAnalysisResources

#Region "Methods"
    ''' <summary>
    ''' Obtains resources necessary for performing analysis results transfer
    ''' </summary>
    ''' <returns>CloseOutType indicating success or failure</returns>
    ''' <remarks>No resources needed for performing results transfer. Function merely meets inheritance requirements</remarks>
    Public Overrides Function GetResources() As CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region

End Class
