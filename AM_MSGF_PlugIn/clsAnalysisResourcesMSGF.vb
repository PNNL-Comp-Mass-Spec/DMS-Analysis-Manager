'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' Created 07/20/2010
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSGF
	Inherits clsAnalysisResources

	'*********************************************************************************************************
    'Manages retrieval of all files needed by MSGF
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
    ''' Gets all files needed by MSGF
	''' </summary>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

		'Get analysis results files
		If GetInputFiles(m_jobParams.GetParam("ResultType")) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'TODO: Handle error
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
    ''' Retrieves input files needed for MSGF
	''' </summary>
	''' <param name="ResultType">String specifying type of analysis results input to extraction process</param>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Private Function GetInputFiles(ByVal ResultType As String) As AnalysisManagerBase.IJobParams.CloseOutType

        Dim eResultType As clsMSGFRunner.ePeptideHitResultType

        Dim DatasetName As String
        Dim RawDataType As String

		Dim FileToGet As String
        Dim blnSuccess As Boolean = False

        ' Cache the dataset name
        DatasetName = m_jobParams.GetParam("DatasetNum")

        ' Make sure the ResultType is valid
        eResultType = clsMSGFRunner.GetPeptideHitResultType(ResultType)

        If eResultType = clsMSGFRunner.ePeptideHitResultType.Sequest OrElse _
           eResultType = clsMSGFRunner.ePeptideHitResultType.XTandem OrElse _
           eResultType = clsMSGFRunner.ePeptideHitResultType.Inspect Then
            blnSuccess = True
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Invalid tool result type (not supported by MSGF): " & ResultType)
            blnSuccess = False
        End If
        
        If Not blnSuccess Then
            Return (IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES)
        End If

        ' Make sure the dataset type is valid
        RawDataType = m_jobParams.GetParam("RawDataType")

        If RawDataType.ToLower <> RAW_DATA_TYPE_DOT_RAW_FILES Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesMSGF.GetResources: Dataset type " & RawDataType & " is not supported; must be " & RAW_DATA_TYPE_DOT_RAW_FILES)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If


        ' Get the Sequest, X!Tandem, or Inspect parameter file
        FileToGet = m_jobParams.GetParam("ParmFileName")
        If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If
        clsGlobal.FilesToDelete.Add(FileToGet)


        ' Get the ModSummary.txt file        
        FileToGet = clsMSGFRunner.GetModSummaryFileName(eResultType, DatasetName)
        If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If
        clsGlobal.FilesToDelete.Add(FileToGet)


        ' Get the Sequest, X!Tandem, or Inspect PHRP results file
        FileToGet = clsMSGFRunner.GetPHRPResultsFileName(eResultType, DatasetName)
        If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If
        clsGlobal.FilesToDelete.Add(FileToGet)


        ' Retrieve the .Raw file so that we can make the .mzXML file prior to running MSGF
        If RetrieveSpectra(RawDataType, m_WorkingDir) Then
            clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_RAW_EXTENSION)            ' Raw file
            clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_MZXML_EXTENSION)          ' mzXML file
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesMSGF.GetResources: Error occurred retrieving spectra.")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If


        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

#End Region

End Class
