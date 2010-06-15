Option Strict On

Imports AnalysisManagerBase
Imports System.Xml.XPath
Imports System.Xml
Imports System.Collections.Specialized

Public Class clsAnalysisResourcesPhosphoFdrAggregator
    Inherits clsAnalysisResources

    Friend Const ASCORE_INPUT_FILE As String = "AScoreBatch.xml"
    Protected WithEvents CmdRunner As clsRunDosProgram

    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        Dim SplitString As String()
        Dim FileNameExt As String()

        SplitString = m_jobParams.GetParam("TargetJobFileList").Split(","c)
        For Each row As String In SplitString
            FileNameExt = row.Split(":"c)
            If FileNameExt(2) = "nocopy" Then
                clsGlobal.m_FilesToDeleteExt.Add(FileNameExt(1))
            End If
        Next

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

        clsGlobal.FilesToDelete.Add(m_jobParams.GetParam("AScoreCIDParamFile"))
        If Not RetrieveFile(m_jobParams.GetParam("AScoreCIDParamFile"), _
         m_jobParams.GetParam("transferFolderPath"), _
         m_mgrParams.GetParam("workdir")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        clsGlobal.FilesToDelete.Add(m_jobParams.GetParam("AScoreETDParamFile"))
        If Not RetrieveFile(m_jobParams.GetParam("AScoreETDParamFile"), _
         m_jobParams.GetParam("transferFolderPath"), _
         m_mgrParams.GetParam("workdir")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        clsGlobal.FilesToDelete.Add(m_jobParams.GetParam("AScoreHCDParamFile"))
        If Not RetrieveFile(m_jobParams.GetParam("AScoreHCDParamFile"), _
         m_jobParams.GetParam("transferFolderPath"), _
         m_mgrParams.GetParam("workdir")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files")

        If Not RetrieveAggregateFiles(SplitString) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Building AScore input file")

        If Not BuildInputFile() Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Dim ext As String
        Dim DumFiles() As String

        'update list of files to be deleted after run
        For Each ext In clsGlobal.m_FilesToDeleteExt
            DumFiles = System.IO.Directory.GetFiles(m_mgrParams.GetParam("workdir"), "*" & ext) 'Zipped DTA
            For Each FileToDel As String In DumFiles
                clsGlobal.FilesToDelete.Add(FileToDel)
            Next
        Next

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function BuildInputFile() As Boolean
        Dim DatasetFiles() As String
        Dim DatasetType As String
        Dim DatasetName As String
        Dim DatasetID As String
        Dim WorkDir As String = m_mgrParams.GetParam("workdir")
        Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(System.IO.Path.Combine(WorkDir, ASCORE_INPUT_FILE))

        Try
            inputFile.WriteLine("<?xml version=""1.0"" encoding=""UTF-8"" ?>")
            inputFile.WriteLine("<ascore_batch>")
            inputFile.WriteLine("  <settings>")
            inputFile.WriteLine("    <max_threads>1</max_threads>")
            inputFile.WriteLine("  </settings>")

            'update list of files to be deleted after run
            DatasetFiles = System.IO.Directory.GetFiles(WorkDir, "*_syn*.txt")
            For Each Dataset As String In DatasetFiles
                Dataset = System.IO.Path.GetFileName(Dataset)
                DatasetType = Dataset.Substring(Dataset.IndexOf("_syn") + 4, 4)
                DatasetName = Dataset.Substring(0, Dataset.Length - (Dataset.Length - Dataset.IndexOf("_syn")))
                inputFile.WriteLine("  <run>")

                DatasetID = GetDatasetID(DatasetName)

                If DatasetType = "_cid" Then
                    inputFile.WriteLine("    <param_file>" & System.IO.Path.Combine(WorkDir, m_jobParams.GetParam("AScoreCIDParamFile")) & "</param_file>")
                ElseIf DatasetType = "_hcd" Then
                    inputFile.WriteLine("    <param_file>" & System.IO.Path.Combine(WorkDir, m_jobParams.GetParam("AScoreHCDParamFile")) & "</param_file>")
                ElseIf DatasetType = "_etd" Then
                    inputFile.WriteLine("    <param_file>" & System.IO.Path.Combine(WorkDir, m_jobParams.GetParam("AScoreETDParamFile")) & "</param_file>")
                End If
                inputFile.WriteLine("    <output_path>" & WorkDir & "</output_path>")
                inputFile.WriteLine("    <dta_file>" & System.IO.Path.Combine(WorkDir, DatasetName & "_dta" & DatasetType & ".txt") & "</dta_file>")
                inputFile.WriteLine("    <fht_file>" & System.IO.Path.Combine(WorkDir, DatasetName & "_fht" & DatasetType & ".txt") & "</fht_file>")
                inputFile.WriteLine("    <syn_file>" & System.IO.Path.Combine(WorkDir, DatasetName & "_syn" & DatasetType & ".txt") & "</syn_file>")
                inputFile.WriteLine("    <dataset_id>" & DatasetID & "</dataset_id>")
                inputFile.WriteLine("  </run>")
            Next

            inputFile.WriteLine("</ascore_batch>")

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating AScore input file" & ex.Message)

        Finally
            inputFile.Close()
        End Try

        Return True

    End Function

    Protected Function GetDatasetID(ByVal DatasetName As String) As String
        Dim Dataset_ID As String = ""
        Dim Dataset_DatasetID As String()

        For Each Item As String In clsGlobal.m_DatasetInfoList
            Dataset_DatasetID = Item.Split(":"c)
            If Dataset_DatasetID(0) = DatasetName Then
                Return Dataset_DatasetID(1)
            End If
        Next

        Return Dataset_ID

    End Function

End Class
