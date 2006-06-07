Imports PRISM.Logging

Public Interface IAnalysisResources

	ReadOnly Property Message() As String

	Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal logger As ILogger)

	Function GetResources() As IJobParams.CloseOutType

End Interface
