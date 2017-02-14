Imports System.Data.SqlClient
Imports System.IO
Imports AnalysisManagerBase
Imports System.Text
Imports System.Text.RegularExpressions

''' <summary>
''' This class reads a DTA_Refinery log file to extract the parent ion mass error information
''' It passes on the information to DMS for storage in table T_Dataset_QC
''' </summary>
''' <remarks></remarks>
Public Class clsDtaRefLogMassErrorExtractor

    Private Const STORE_MASS_ERROR_STATS_SP_NAME As String = "StoreDTARefMassErrorStats"

    Private ReadOnly m_mgrParams As IMgrParams
    Private ReadOnly m_WorkDir As String
    Private ReadOnly m_DebugLevel As Short
    Private ReadOnly mPostResultsToDB As Boolean

    Private mErrorMessage As String

    Private Structure udtMassErrorInfoType
        Public DatasetName As String
        Public DatasetID As Integer
        Public PSMJob As Integer
        Public MassErrorPPM As Double               ' Parent Ion Mass Error, before refinement
        Public MassErrorPPMRefined As Double        ' Parent Ion Mass Error, after refinement
    End Structure

    Public ReadOnly Property ErrorMessage() As String
        Get
            Return mErrorMessage
        End Get
    End Property

    Public Sub New(mgrParams As IMgrParams, strWorkDir As String, intDebugLevel As Short, blnPostResultsToDB As Boolean)

        m_mgrParams = mgrParams
        m_WorkDir = strWorkDir
        m_DebugLevel = intDebugLevel
        mPostResultsToDB = blnPostResultsToDB

        mErrorMessage = String.Empty
    End Sub

    Private Function ConstructXML(udtMassErrorInfo As udtMassErrorInfoType) As String
        Dim sbXml = New StringBuilder()

        Try
            sbXml.Append("<DTARef_MassErrorStats>")

            sbXml.Append((Convert.ToString("<Dataset>") & udtMassErrorInfo.DatasetName) + "</Dataset>")
            sbXml.Append((Convert.ToString("<PSM_Source_Job>") & udtMassErrorInfo.PSMJob) + "</PSM_Source_Job>")

            sbXml.Append("<Measurements>")
            sbXml.Append((Convert.ToString("<Measurement Name=""" + "MassErrorPPM" + """>") & udtMassErrorInfo.MassErrorPPM) + "</Measurement>")
            sbXml.Append((Convert.ToString("<Measurement Name=""" + "MassErrorPPM_Refined" + """>") & udtMassErrorInfo.MassErrorPPMRefined) + "</Measurement>")
            sbXml.Append("</Measurements>")

            sbXml.Append("</DTARef_MassErrorStats>")

        Catch ex As Exception
            Console.WriteLine("Error converting Mass Error stats to XML; details:")
            Console.WriteLine(ex)
            Return String.Empty
        End Try

        Return sbXml.ToString()

    End Function

    Public Function ParseDTARefineryLogFile(strDatasetName As String, intDatasetID As Integer, intPSMJob As Integer) As Boolean
        Return ParseDTARefineryLogFile(strDatasetName, intDatasetID, intPSMJob, m_WorkDir)
    End Function

    Public Function ParseDTARefineryLogFile(strDatasetName As String, intDatasetID As Integer, intPSMJob As Integer, strWorkDirPath As String) As Boolean

        Dim fiSourceFile As FileInfo

        Dim blnOriginalDistributionSection = False
        Dim blnRefinedDistributionSection = False

        Dim udtMassErrorInfo As udtMassErrorInfoType

        Dim reMassError = New Regex("Robust estimate[ \t]+([^\t ]+)", RegexOptions.Compiled)
        Dim reMatch As Match

        Dim strLineIn As String

        Try

            udtMassErrorInfo = New udtMassErrorInfoType
            udtMassErrorInfo.DatasetName = strDatasetName
            udtMassErrorInfo.DatasetID = intDatasetID
            udtMassErrorInfo.PSMJob = intPSMJob
            udtMassErrorInfo.MassErrorPPM = Double.MinValue
            udtMassErrorInfo.MassErrorPPMRefined = Double.MinValue

            fiSourceFile = New FileInfo(Path.Combine(strWorkDirPath, strDatasetName & "_dta_DtaRefineryLog.txt"))
            If Not fiSourceFile.Exists Then
                mErrorMessage = "DtaRefinery Log file not found"
                Return False
            End If

            Using srSourceFile = New StreamReader(New FileStream(fiSourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While Not srSourceFile.EndOfStream
                    strLineIn = srSourceFile.ReadLine()

                    If strLineIn.Contains("ORIGINAL parent ion mass error distribution") Then
                        blnOriginalDistributionSection = True
                        blnRefinedDistributionSection = False
                    End If

                    If strLineIn.Contains("REFINED parent ion mass error distribution") Then
                        blnOriginalDistributionSection = False
                        blnRefinedDistributionSection = True
                    End If

                    If strLineIn.StartsWith("Robust estimate") Then
                        reMatch = reMassError.Match(strLineIn)

                        Dim dblMassError As Double
                        If reMatch.Success Then
                            If Double.TryParse(reMatch.Groups(1).Value, dblMassError) Then

                                If blnOriginalDistributionSection Then
                                    udtMassErrorInfo.MassErrorPPM = dblMassError
                                End If

                                If blnRefinedDistributionSection Then
                                    udtMassErrorInfo.MassErrorPPMRefined = dblMassError
                                End If
                            Else
                                mErrorMessage = "Unable to extract mass error value from 'Robust estimate' line in the DTA Refinery log file; RegEx capture is not a number: " & reMatch.Groups(1).Value
                                Return False
                            End If
                        Else
                            mErrorMessage = "Unable to extract mass error value from 'Robust estimate' line in the DTA Refinery log file; RegEx match failed"
                            Return False
                        End If
                    End If

                Loop

            End Using

            If udtMassErrorInfo.MassErrorPPM > Double.MinValue Then

                Dim strXMLResults As String

                strXMLResults = ConstructXML(udtMassErrorInfo)

                If mPostResultsToDB Then
                    Dim blnSuccess As Boolean

                    blnSuccess = PostMassErrorInfoToDB(intDatasetID, strXMLResults)

                    If Not blnSuccess Then
                        If String.IsNullOrEmpty(mErrorMessage) Then
                            mErrorMessage = "Unknown error posting Mass Error results from DTA Refinery to the database"
                        End If
                        Return False
                    End If
                End If

            End If


        Catch ex As Exception
            mErrorMessage = "Exception in ParseDTARefineryLogFile: " & ex.Message
            Return False
        End Try

        Return True

    End Function


    Private Function PostMassErrorInfoToDB(
      intDatasetID As Integer,
      strXMLResults As String) As Boolean

        Const MAX_RETRY_COUNT = 3

        Dim objCommand As SqlCommand

        Dim blnSuccess As Boolean

        Try

            ' Call stored procedure STORE_MASS_ERROR_STATS_SP_NAME in DMS5

            objCommand = New SqlCommand()

            With objCommand
                .CommandType = CommandType.StoredProcedure
                .CommandText = STORE_MASS_ERROR_STATS_SP_NAME

                .Parameters.Add(New SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue
                .Parameters.Add(New SqlParameter("@DatasetID", SqlDbType.Int)).Value = intDatasetID
                .Parameters.Add(New SqlParameter("@ResultsXML", SqlDbType.Xml)).Value = strXMLResults
            End With


            Dim objAnalysisTask = New clsAnalysisJob(m_mgrParams, m_DebugLevel)

            'Execute the SP (retry the call up to 4 times)
            Dim ResCode As Integer
            ResCode = objAnalysisTask.DMSProcedureExecutor.ExecuteSP(objCommand, MAX_RETRY_COUNT)

            If ResCode = 0 Then
                blnSuccess = True
            Else
                mErrorMessage = "Error storing DTA Refinery Mass Error Results in the database, " & STORE_MASS_ERROR_STATS_SP_NAME & " returned " & ResCode.ToString
                blnSuccess = False
            End If

        Catch ex As Exception
            mErrorMessage = "Exception storing DTA Refinery Mass Error Results in the database: " & ex.Message
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

End Class
