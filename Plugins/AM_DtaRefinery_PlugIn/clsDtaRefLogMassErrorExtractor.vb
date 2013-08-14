Imports AnalysisManagerBase
Imports System.Text

''' <summary>
''' This class reads a DTA_Refinery log file to extract the paren ion mass error information
''' </summary>
''' <remarks></remarks>
Public Class clsDtaRefLogMassErrorExtractor

	Protected Const STORE_MASS_ERROR_STATS_SP_NAME As String = "StoreDTARefMassErrorStats"

	Protected m_mgrParams As IMgrParams
	Protected m_WorkDir As String
	Protected m_DebugLevel As Integer
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

	Public Sub New(ByRef mgrParams As IMgrParams, ByVal strWorkDir As String, ByVal intDebugLevel As Integer, ByVal blnPostResultsToDB As Boolean)

		m_mgrParams = mgrParams
		m_WorkDir = strWorkDir
		m_DebugLevel = intDebugLevel
		mPostResultsToDB = blnPostResultsToDB

		mErrorMessage = String.Empty
	End Sub

	Protected Function ConstructXML(ByVal udtMassErrorInfo As udtMassErrorInfoType) As String
		Dim sbXml = New System.Text.StringBuilder()

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

	Public Function ParseDTARefineryLogFile(ByVal strDatasetName As String, ByVal intDatasetID As Integer, ByVal intPSMJob As Integer) As Boolean
		Return ParseDTARefineryLogFile(strDatasetName, intDatasetID, intPSMJob, m_WorkDir)
	End Function

	Public Function ParseDTARefineryLogFile(ByVal strDatasetName As String, ByVal intDatasetID As Integer, ByVal intPSMJob As Integer, ByVal strWorkDirPath As String) As Boolean

		Dim fiSourceFile As System.IO.FileInfo

		Dim blnOriginalDistributionSection As Boolean = False
		Dim blnRefinedDistributionSection As Boolean = False

		Dim udtMassErrorInfo As udtMassErrorInfoType

		Dim reMassError = New RegularExpressions.Regex("Robust estimate[ \t]+([^\t ]+)", RegularExpressions.RegexOptions.Compiled)
		Dim reMatch As RegularExpressions.Match

		Dim strLineIn As String

		Try

			udtMassErrorInfo = New udtMassErrorInfoType
			udtMassErrorInfo.DatasetName = strDatasetName
			udtMassErrorInfo.DatasetID = intDatasetID
			udtMassErrorInfo.PSMJob = intPSMJob
			udtMassErrorInfo.MassErrorPPM = Double.MinValue
			udtMassErrorInfo.MassErrorPPMRefined = Double.MinValue

			fiSourceFile = New System.IO.FileInfo(System.IO.Path.Combine(strWorkDirPath, strDatasetName & "_dta_DtaRefineryLog.txt"))
			If Not fiSourceFile.Exists Then
				mErrorMessage = "DtaRefinery Log file not found"
				Return False
			End If

			Using srSourceFile = New System.IO.StreamReader(New System.IO.FileStream(fiSourceFile.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

				Do While srSourceFile.Peek > -1
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
					Dim strConnectionString As String = m_mgrParams.GetParam("connectionstring")
					Dim blnSuccess As Boolean

					blnSuccess = PostMassErrorInfoToDB(intDatasetID, strXMLResults, strConnectionString, STORE_MASS_ERROR_STATS_SP_NAME)

					If Not blnSuccess Then
						If String.IsNullOrEmpty(mErrorMessage) Then
							mErrorMessage = "Unknown error posting Mass Error results from DTA Refinery to the database"
						End If
						Return False
					End If
				End If

			End If


		Catch ex As Exception
			mErrorMessage = "Exception in ValidateDTARefineryLogFile"
			Return False
		End Try

		Return True

	End Function


	Protected Function PostMassErrorInfoToDB(
	  ByVal intDatasetID As Integer,
	  ByVal strXMLResults As String,
	  ByVal strConnectionString As String,
	  ByVal strStoredProcedure As String) As Boolean

		Const MAX_RETRY_COUNT As Integer = 3

		Dim objCommand As System.Data.SqlClient.SqlCommand

		Dim blnSuccess As Boolean

		Try

			' Call stored procedure strStoredProcedure using connection string strConnectionString

			If String.IsNullOrWhiteSpace(strConnectionString) Then
				mErrorMessage = "Connection string empty in PostMassErrorInfoToDB"
				Return False
			End If

			If String.IsNullOrWhiteSpace(strStoredProcedure) Then
				strStoredProcedure = STORE_MASS_ERROR_STATS_SP_NAME
			End If

			objCommand = New System.Data.SqlClient.SqlCommand()

			With objCommand
				.CommandType = CommandType.StoredProcedure
				.CommandText = strStoredProcedure

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
			ResCode = objAnalysisTask.ExecuteSP(objCommand, strConnectionString, MAX_RETRY_COUNT)

			objAnalysisTask = Nothing

			If ResCode = 0 Then
				blnSuccess = True
			Else
				mErrorMessage = "Error storing DTA Refinery Mass Error Results in the database, " & strStoredProcedure & " returned " & ResCode.ToString
				blnSuccess = False
			End If

		Catch ex As System.Exception
			mErrorMessage = "Exception storing DTA Refinery Mass Error Results in the database: " & ex.Message
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

End Class
