'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/15/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSPathFinder
	Inherits clsAnalysisResources

	Public Overrides Sub Setup(ByRef mgrParams As IMgrParams, ByRef jobParams As IJobParams)
		MyBase.Setup(mgrParams, jobParams)
		SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
	End Sub

	Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile)
		MyBase.Setup(mgrParams, jobParams, statusTools)
		SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
	End Sub

	Public Overrides Function GetResources() As IJobParams.CloseOutType

		If Not RetrieveFastaAndParamFile() Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		Dim eResult = RetrieveInstrumentData()
		If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return eResult
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function RetrieveFastaAndParamFile() As Boolean

		Dim currentTask As String = "Initializing"

		Try

			' Retrieve the Fasta file
			Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")

			currentTask = "RetrieveOrgDB to " & localOrgDbFolder

			If Not RetrieveOrgDB(localOrgDbFolder) Then Return False

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

			' Retrieve the parameter file
			' This will also obtain the _ModDefs.txt file using query 
			'  SELECT Local_Symbol, Monoisotopic_Mass_Correction, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
			'  FROM V_Param_File_Mass_Mod_Info 
			'  WHERE Param_File_Name = 'ParamFileName'

			Dim paramFileName = m_jobParams.GetParam("ParmFileName")
			Dim paramFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath")

			currentTask = "RetrieveGeneratedParamFile " & paramFileName

			If Not RetrieveGeneratedParamFile(paramFileName, paramFileStoragePath) Then
				Return False
			End If

			Return True

		Catch ex As Exception
			m_message = "Exception in RetrieveFastaAndParamFile: " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; task = " & currentTask & "; " & clsGlobal.GetExceptionStackTrace(ex))
			Return False
		End Try

	End Function

	Protected Function RetrieveInstrumentData() As IJobParams.CloseOutType

		Dim currentTask As String = "Initializing"

		Try
			' Retrieve the .pbf file from the MSXml cache folder

			currentTask = "RetrievePBFFile"

			Dim eResult = GetPBFFile()
			If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return eResult
			End If

			m_jobParams.AddResultFileExtensionToSkip(DOT_PBF_EXTENSION)

			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		Catch ex As Exception
			m_message = "Exception in RetrieveInstrumentData: " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; task = " & currentTask & "; " & clsGlobal.GetExceptionStackTrace(ex))
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

End Class
