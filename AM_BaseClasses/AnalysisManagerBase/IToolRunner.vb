Imports PRISM.Logging

Public Interface IToolRunner

	Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
			ByVal logger As ILogger, ByVal StatusTools As IStatusFile)

	ReadOnly Property ResFolderName() As String


	' explanation of what happened to last operation this class performed
	ReadOnly Property Message() As String

	' the state of completion of the job (as a percentage)
	ReadOnly Property Progress() As Single


	Function RunTool() As IJobParams.CloseOutType

End Interface
