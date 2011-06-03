Option Strict On

Imports AnalysisManagerBase
Imports System.Xml.XPath
Imports System.Xml
Imports System.Collections.Specialized
Imports System.IO

Public Class clsAnalysisResourcesMultiAlignAggregator
    Inherits clsAnalysisResources

    Friend Const MULTIALIGN_INPUT_FILE As String = "Input.txt"
    Protected WithEvents CmdRunner As clsRunDosProgram

    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        Dim SplitString As String()
        Dim FileNameExt As String()
        Dim strMAParamFileName As String
        Dim strMAParameterFileStoragePath As String
        Dim strParamFileStoragePathKeyName As String

        SplitString = m_jobParams.GetParam("TargetJobFileList").Split(","c)
        For Each row As String In SplitString
            FileNameExt = row.Split(":"c)
            If FileNameExt(2) = "nocopy" Then
                clsGlobal.m_FilesToDeleteExt.Add(FileNameExt(1))
            End If
        Next

        ' Retrieve the MultiAlign Parameter .xml file specified for this job
        strMAParamFileName = m_jobParams.GetParam("ParmFileName")
        If strMAParamFileName Is Nothing OrElse strMAParamFileName.Length = 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MultiAlign ParmFileName not defined in the settings for this job; unable to continue")
            Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If

        strParamFileStoragePathKeyName = AnalysisManagerBase.clsAnalysisMgrSettings.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "MultiAlign"
        strMAParameterFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName)
        If strMAParameterFileStoragePath Is Nothing OrElse strMAParameterFileStoragePath.Length = 0 Then
            strMAParameterFileStoragePath = "\\gigasax\DMS_Parameter_Files\MultiAlign"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & strParamFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & strMAParameterFileStoragePath)
        End If

        If Not CopyFileToWorkDir(strMAParamFileName, strMAParameterFileStoragePath, m_WorkingDir) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files")

        If Not RetrieveAggregateFiles(SplitString) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Building MultiAlign input file")

        ' Build the MultiAlign input text file
        If Not BuildMultiAlignInputTextFile() Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function


    Protected Function BuildMultiAlignInputTextFile() As Boolean

        Const INPUT_FILENAME As String = "input.txt"

        Dim result As Boolean = True
        Dim swOutFile As System.IO.StreamWriter

        Dim TargetFilePath As String = System.IO.Path.Combine(m_WorkingDir, INPUT_FILENAME)

        Dim blnInputFileDefined As Boolean
        Dim blnOutputDirectoryDefined As Boolean

        Dim SplitString As String()
        Dim FileNameExt As String()
        SplitString = m_jobParams.GetParam("TargetJobFileList").Split(","c)
        For Each row As String In SplitString
            FileNameExt = row.Split(":"c)
            If FileNameExt(2) = "nocopy" Then
                clsGlobal.m_FilesToDeleteExt.Add(FileNameExt(1))
            End If
        Next

        Dim TmpFile As String
        Dim Files As String()

        blnInputFileDefined = False
        blnOutputDirectoryDefined = False
        result = True

        ' Create the MA input file 
        Try

            swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(TargetFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

            Files = Directory.GetFiles(m_WorkingDir, "*" & FileNameExt(1))

            swOutFile.WriteLine("[Files]")
            Dim AlignmentDataset As String = m_jobParams.GetParam("AlignmentDataset")
            For Each TmpFile In Files
                If Not String.IsNullOrEmpty(AlignmentDataset.Trim) AndAlso TmpFile.Contains(AlignmentDataset) Then
                    'Add the * to the dataset to align to.
                    swOutFile.WriteLine(TmpFile & "*")
                Else
                    swOutFile.WriteLine(TmpFile)
                    '..\SARC_MS_Final\663878_Sarc_MS_13_24Aug10_Cheetah_10-08-02_0000_LCMSFeatures.txt
                End If
            Next

            'Check to see if a mass tag database has been defined and NO alignment dataset has been defined
            Dim AmtDb As String = m_jobParams.GetParam("AMTDB")
            If Not String.IsNullOrEmpty(AmtDb.Trim) Then
                swOutFile.WriteLine("[Database]")
                swOutFile.WriteLine("Database = " & m_jobParams.GetParam("AMTDB"))
                swOutFile.WriteLine("Server = " & m_jobParams.GetParam("AMTDBServer"))
                'Database = MT_Human_Sarcopenia_MixedLC_P692
                'Server = elmer
            End If

            swOutFile.Close()

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesMultiAlign.BuildMultiAlignInputTextFile, Error buliding the input .txt file (" & INPUT_FILENAME & "): " & ex.Message)
            result = False

        End Try

        Return result
    End Function

End Class
