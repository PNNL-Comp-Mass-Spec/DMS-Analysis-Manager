Imports AnalysisManagerBase
Imports System.IO
Imports System.Text
Imports System.Runtime.InteropServices

''' <summary>
''' This class reads the console text from the PPMErrorCharter's console output and extracts the parent ion mass error information
''' It passes on the information to DMS for storage in table T_Dataset_QC
''' </summary>
''' <remarks></remarks>
Public Class clsMzRefineryMassErrorStatsExtractor

    Protected Const STORE_MASS_ERROR_STATS_SP_NAME As String = "StoreDTARefMassErrorStats"

    Protected m_mgrParams As IMgrParams
    Protected m_WorkDir As String
    Protected m_DebugLevel As Short
    Protected mPostResultsToDB As Boolean

    Protected mErrorMessage As String

    Protected Structure udtMassErrorInfoType
        Public DatasetName As String
        Public DatasetID As Integer
        Public PSMJob As Integer
        Public MassErrorPPM As Double				' Parent Ion Mass Error, before refinement
        Public MassErrorPPMRefined As Double		' Parent Ion Mass Error, after refinement
    End Structure

    Public ReadOnly Property ErrorMessage() As String
        Get
            Return mErrorMessage
        End Get
    End Property

    Public Sub New(ByRef mgrParams As IMgrParams, strWorkDir As String, intDebugLevel As Short, blnPostResultsToDB As Boolean)

        m_mgrParams = mgrParams
        m_WorkDir = strWorkDir
        m_DebugLevel = intDebugLevel
        mPostResultsToDB = blnPostResultsToDB

        mErrorMessage = String.Empty
    End Sub

    Protected Function ConstructXML(udtMassErrorInfo As udtMassErrorInfoType) As String
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

    Public Function ParsePPMErrorCharterOutput(
      strDatasetName As String,
      intDatasetID As Integer,
      intPSMJob As Integer,
      ppmErrorCharterConsoleOutputFilePath As String) As Boolean

        ' Parse the Console Output file to extract the mass error reported in this table
        '
        ' Using fixed data file "E:\DMS_WorkDir\Pcarb001_LTQFT_run1_23Sep05_Andro_0705-06_FIXED.mzML"
        ' Statistic                   Original    Refined
        ' MeanMassErrorPPM:              2.430      1.361
        ' MedianMassErrorPPM:            1.782      0.704
        ' StDev(Mean):                   6.969      6.972
        ' StDev(Median):                 6.999      7.003
        ' PPM Window for 99%: 0 +/-     22.779     21.712
        ' PPM Window for 99%: high:     22.779     21.712
        ' PPM Window for 99%:  low:    -19.216    -20.305

        Const MASS_ERROR_PPM As String = "MedianMassErrorPPM:"

        Try

            Dim udtMassErrorInfo = New udtMassErrorInfoType
            udtMassErrorInfo.DatasetName = strDatasetName
            udtMassErrorInfo.DatasetID = intDatasetID
            udtMassErrorInfo.PSMJob = intPSMJob
            udtMassErrorInfo.MassErrorPPM = Double.MinValue
            udtMassErrorInfo.MassErrorPPMRefined = Double.MinValue

            Dim fiSourceFile = New FileInfo(ppmErrorCharterConsoleOutputFilePath)
            If Not fiSourceFile.Exists Then
                mErrorMessage = "MzRefinery Log file not found"
                Return False
            End If

            Using srSourceFile = New StreamReader(New FileStream(fiSourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))

                Do While Not srSourceFile.EndOfStream
                    Dim strLineIn = srSourceFile.ReadLine()

                    If String.IsNullOrWhiteSpace(strLineIn) Then Continue Do

                    strLineIn = strLineIn.Trim()

                    Dim massError As Double

                    If strLineIn.StartsWith(MASS_ERROR_PPM) Then
                        Dim dataString = strLineIn.Substring(MASS_ERROR_PPM.Length).Trim()
                        massError = 0

                        Dim dataValues = dataString.Split(" "c).ToList()

                        If Double.TryParse(dataValues.First, massError) Then
                            udtMassErrorInfo.MassErrorPPM = massError
                        End If

                        If dataValues.Count > 1 AndAlso Double.TryParse(dataValues.Last, massError) Then
                            udtMassErrorInfo.MassErrorPPMRefined = massError
                        End If

                    End If
                Loop

            End Using

            If Math.Abs(udtMassErrorInfo.MassErrorPPM - Double.MinValue) < Single.Epsilon Then
                mErrorMessage = "Did not find '" & MASS_ERROR_PPM & "' in the PPM Error Charter output"
                Return False
            End If

            If Math.Abs(udtMassErrorInfo.MassErrorPPMRefined - Double.MinValue) < Single.Epsilon Then
                mErrorMessage = "Did not find '" & MASS_ERROR_PPM & "' with two values in the PPM Error Charter output"
                Return False
            End If

            Dim strXMLResults = ConstructXML(udtMassErrorInfo)

            If mPostResultsToDB Then
                Dim blnSuccess As Boolean

                blnSuccess = PostMassErrorInfoToDB(intDatasetID, strXMLResults)

                If Not blnSuccess Then
                    If String.IsNullOrEmpty(mErrorMessage) Then
                        mErrorMessage = "Unknown error posting Mass Error results from MzRefinery to the database"
                    End If
                    Return False
                End If
            End If

        Catch ex As Exception
            mErrorMessage = "Exception in ParsePPMErrorCharterOutput: " & ex.Message
            Return False
        End Try

        Return True

    End Function

    Protected Function PostMassErrorInfoToDB(
      intDatasetID As Integer,
      strXMLResults As String) As Boolean

        Const MAX_RETRY_COUNT As Integer = 3

        Dim objCommand As System.Data.SqlClient.SqlCommand

        Dim blnSuccess As Boolean

        Try

            ' Call stored procedure StoreDTARefMassErrorStats in DMS5

            objCommand = New System.Data.SqlClient.SqlCommand()

            With objCommand
                .CommandType = CommandType.StoredProcedure
                .CommandText = STORE_MASS_ERROR_STATS_SP_NAME

                .Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
                .Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

                .Parameters.Add(New SqlClient.SqlParameter("@DatasetID", SqlDbType.Int))
                .Parameters.Item("@DatasetID").Direction = ParameterDirection.Input
                .Parameters.Item("@DatasetID").Value = intDatasetID

                .Parameters.Add(New SqlClient.SqlParameter("@ResultsXML", SqlDbType.Xml))
                .Parameters.Item("@ResultsXML").Direction = ParameterDirection.Input
                .Parameters.Item("@ResultsXML").Value = strXMLResults
            End With


            Dim objAnalysisTask = New clsAnalysisJob(m_mgrParams, m_DebugLevel)

            'Execute the SP (retry the call up to 4 times)
            Dim ResCode As Integer
            ResCode = objAnalysisTask.DMSProcedureExecutor.ExecuteSP(objCommand, MAX_RETRY_COUNT)

            objAnalysisTask = Nothing

            If ResCode = 0 Then
                blnSuccess = True
            Else
                mErrorMessage = "Error storing MzRefinery Mass Error Results in the database, " & STORE_MASS_ERROR_STATS_SP_NAME & " returned " & ResCode.ToString
                blnSuccess = False
            End If

        Catch ex As System.Exception
            mErrorMessage = "Exception storing MzRefinery Mass Error Results in the database: " & ex.Message
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

End Class
