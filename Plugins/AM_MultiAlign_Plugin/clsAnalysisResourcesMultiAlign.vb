'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2010, Battelle Memorial Institute
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMultiAlign
    Inherits clsAnalysisResources


    Public Overrides Function GetResources() As IJobParams.CloseOutType

        Dim strFileToGet As String
        Dim strMAParamFileName As String
        Dim strMAParameterFileStoragePath As String
        Dim strParamFileStoragePathKeyName As String

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting required files")

        Dim SplitString As String()
        Dim FileNameExt As String()
        Dim strInputFileExtension As String = String.Empty

        SplitString = m_jobParams.GetParam("TargetJobFileList").Split(","c)
        For Each row As String In SplitString
            FileNameExt = row.Split(":"c)
            If FileNameExt.Length < 3 Then
                Throw New InvalidOperationException("Malformed target job specification; must have three columns separated by two colons: " & row)
            End If
            If FileNameExt(2) = "nocopy" Then
                m_JobParams.AddResultFileExtensionToSkip(FileNameExt(1))
            End If
            strInputFileExtension = FileNameExt(1)
        Next

        ' Retrieve FeatureFinder _LCMSFeatures.txt or Decon2ls isos file for this dataset
        strFileToGet = m_DatasetName & strInputFileExtension
        If Not FindAndRetrieveMiscFiles(strFileToGet, False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Retrieve the MultiAlign Parameter .xml file specified for this job
        strMAParamFileName = m_jobParams.GetParam("ParmFileName")
        If strMAParamFileName Is Nothing OrElse strMAParamFileName.Length = 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MultiAlign ParmFileName not defined in the settings for this job; unable to continue")
            Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If

        strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "MultiAlign"
        strMAParameterFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName)
        If strMAParameterFileStoragePath Is Nothing OrElse strMAParameterFileStoragePath.Length = 0 Then
            strMAParameterFileStoragePath = "\\gigasax\DMS_Parameter_Files\MultiAlign"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & strParamFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & strMAParameterFileStoragePath)
        End If

        If Not CopyFileToWorkDir(strMAParamFileName, strMAParameterFileStoragePath, m_WorkingDir) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Build the MultiAlign input text file
        Dim blnSuccess As Boolean
        blnSuccess = BuildMultiAlignInputTextFile(strInputFileExtension)

        If Not blnSuccess Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function BuildMultiAlignInputTextFile(ByVal strInputFileExtension As String) As Boolean

        Const INPUT_FILENAME As String = "input.txt"

        Dim blnSuccess As Boolean = True
        Dim swOutFile As System.IO.StreamWriter

        Dim TargetFilePath As String = System.IO.Path.Combine(m_WorkingDir, INPUT_FILENAME)
        Dim DatasetFilePath As String = System.IO.Path.Combine(m_WorkingDir, m_DatasetName & strInputFileExtension)

        Dim blnInputFileDefined As Boolean
        Dim blnOutputDirectoryDefined As Boolean

        blnInputFileDefined = False
        blnOutputDirectoryDefined = False
        blnSuccess = True

        ' Create the MA input file 
        Try

            swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(TargetFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

            swOutFile.WriteLine("[Files]")
            swOutFile.WriteLine(DatasetFilePath)
            '..\SARC_MS_Final\663878_Sarc_MS_13_24Aug10_Cheetah_10-08-02_0000_LCMSFeatures.txt

            swOutFile.WriteLine("[Database]")

            swOutFile.WriteLine("Database = " & m_jobParams.GetParam("AMTDB"))
            swOutFile.WriteLine("Server = " & m_jobParams.GetParam("AMTDBServer"))
            'Database = MT_Human_Sarcopenia_MixedLC_P692
            'Server = elmer

            swOutFile.Close()

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesMultiAlign.BuildMultiAlignInputTextFile, Error buliding the input .txt file (" & INPUT_FILENAME & "): " & ex.Message)
            blnSuccess = False
        End Try

        Return blnSuccess
    End Function

End Class
