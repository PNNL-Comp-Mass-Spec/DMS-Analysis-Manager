' Last modified 06/15/2009 JDS - Added logging using log4net
Imports AnalysisManagerBase
Imports ParamFileGenerator.MakeParams

Public Class clsAnalysisResourcesMASIC
    Inherits clsAnalysisResources

#Region "Methods"
    ''' <summary>
    ''' Retrieves files necessary for performance of Sequest analysis
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        Dim strWorkDir As String = m_mgrParams.GetParam("workdir")

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        ' Get input data file
        Dim CreateStoragePathInfoOnly As Boolean = False
        Dim RawDataType As String = m_jobParams.GetParam("RawDataType")

        Select Case RawDataType.ToLower
            Case RAW_DATA_TYPE_DOT_RAW_FILES, _
                 RAW_DATA_TYPE_DOT_WIFF_FILES, _
                 RAW_DATA_TYPE_DOT_UIMF_FILES, _
                 RAW_DATA_TYPE_DOT_MZXML_FILES, _
                 RAW_DATA_TYPE_DOT_D_FOLDERS

                ' If desired, set the following to True to not actually copy the .Raw 
                ' (or .wiff, .uimf, etc.) file locally, and instead determine where it is 
                ' located, then create a text file named "DatesetName.raw_StoragePathInfo.txt"
                ' This file would contain just one line of text: the full path to the actual file

                ' However, we have found that this can create undo strain on the storage servers (or NWFS Archive)
                ' Thus, we are now setting this to False
                CreateStoragePathInfoOnly = False
            Case Else
                CreateStoragePathInfoOnly = False
        End Select

        If Not RetrieveSpectra(RawDataType, strWorkDir, CreateStoragePathInfoOnly) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Add additional extensions to delete after the tool finishes
        clsGlobal.m_FilesToDeleteExt.Add("_StoragePathInfo.txt")

        ' We'll add the following extensions to m_FilesToDeleteExt
        ' Note, though, that the DeleteDataFile function will delete the .Raw or .mgf/.cdf files
        clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_WIFF_EXTENSION)
        clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_RAW_EXTENSION)
        clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_UIMF_EXTENSION)
        clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_MZXML_EXTENSION)

        clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_MGF_EXTENSION)
        clsGlobal.m_FilesToDeleteExt.Add(clsAnalysisResources.DOT_CDF_EXTENSION)

        'Retrieve param file
        If Not RetrieveFile( _
         m_jobParams.GetParam("ParmFileName"), _
         m_jobParams.GetParam("ParmFileStoragePath"), _
         strWorkDir) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'All finished
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region

End Class
