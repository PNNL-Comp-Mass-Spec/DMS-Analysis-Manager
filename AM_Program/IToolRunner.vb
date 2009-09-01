'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 07/29/2008
'*********************************************************************************************************

Namespace AnalysisManagerBase

    Public Interface IToolRunner

        '*********************************************************************************************************
        'Insert general class description here
        '*********************************************************************************************************

#Region "Properties"
        ReadOnly Property ResFolderName() As String

        ' explanation of what happened to last operation this class performed
        ReadOnly Property Message() As String

        ' the state of completion of the job (as a percentage)
        ReadOnly Property Progress() As Single
#End Region

#Region "Methods"
        Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal StatusTools As IStatusFile)

        Function RunTool() As IJobParams.CloseOutType

        Sub SetResourcerDataFileList(ByVal DataFileList() As String)
#End Region

    End Interface

End Namespace
