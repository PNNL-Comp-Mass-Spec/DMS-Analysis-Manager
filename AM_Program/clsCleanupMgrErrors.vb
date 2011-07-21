Option Strict On

Public Class clsCleanupMgrErrors

#Region "Constants"

    Protected Const SP_NAME_REPORTMGRCLEANUP As String = "ReportManagerErrorCleanup"

    Public Enum eCleanupModeConstants
        Disabled = 0
        CleanupOnce = 1
        CleanupAlways = 2
    End Enum

    Public Enum eCleanupActionCodeConstants
        Start = 1
        Success = 2
        Fail = 3
    End Enum
#End Region

#Region "Class wide Variables"
    Protected mInitialized As Boolean = False

    Protected mMgrConfigDBConnectionString As String = String.Empty
    Protected mManagerName As String = String.Empty

    Protected mMgrFolderPath As String = String.Empty
    Protected mWorkingDirPath As String = String.Empty

#End Region

    Public Sub New(ByVal strMgrConfigDBConnectionString As String, _
                   ByVal strManagerName As String, _
                   ByVal strMgrFolderPath As String, _
                   ByVal strWorkingDirPath As String)

        If strMgrConfigDBConnectionString Is Nothing OrElse strMgrConfigDBConnectionString.Length = 0 Then
            Throw New Exception("Manager config DB connection string is not defined")
        ElseIf strManagerName Is Nothing OrElse strManagerName.Length = 0 Then
            Throw New Exception("Manager name is not defined")
        Else
            mMgrConfigDBConnectionString = String.Copy(strMgrConfigDBConnectionString)
            mManagerName = String.Copy(strManagerName)

            mMgrFolderPath = strMgrFolderPath
            mWorkingDirPath = strWorkingDirPath

            mInitialized = True
        End If

    End Sub

    Public Function AutoCleanupManagerErrors(ByVal eManagerErrorCleanupMode As eCleanupModeConstants, ByVal DebugLevel As Integer) As Boolean
        Dim blnSuccess As Boolean
        Dim strFailureMessage As String = String.Empty

        If Not mInitialized Then Return False

        If eManagerErrorCleanupMode <> eCleanupModeConstants.Disabled Then

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Attempting to automatically clean the work directory")

            ' Call SP ReportManagerErrorCleanup @ActionCode=1
            ReportManagerErrorCleanup(eCleanupActionCodeConstants.Start)

            ' Delete all folders and subfolders in work folder
            blnSuccess = AnalysisManagerBase.clsGlobal.CleanWorkDir(mWorkingDirPath, 1, strFailureMessage)

            If Not blnSuccess Then
                If strFailureMessage Is Nothing OrElse strFailureMessage.Length = 0 Then
                    strFailureMessage = "unable to clear work directory"
                End If
            Else
                ' If successful, then deletes flag files: flagfile.txt and flagFile_Svr.txt
                blnSuccess = AnalysisManagerBase.clsGlobal.DeleteDeconServerFlagFile(DebugLevel)

                If Not blnSuccess Then
                    strFailureMessage = "error deleting " & AnalysisManagerBase.clsGlobal.DECON_SERVER_FLAG_FILE_NAME
                Else
                    blnSuccess = AnalysisManagerBase.clsGlobal.DeleteStatusFlagFile(DebugLevel)
                    If Not blnSuccess Then
                        strFailureMessage = "error deleting " & AnalysisManagerBase.clsGlobal.FLAG_FILE_NAME
                    End If
                End If
            End If


            ' If successful, then call SP with ReportManagerErrorCleanup @ActionCode=2 
            '    otherwise call SP ReportManagerErrorCleanup with @ActionCode=3

            If blnSuccess Then
                ReportManagerErrorCleanup(eCleanupActionCodeConstants.Success)
            Else
                ReportManagerErrorCleanup(eCleanupActionCodeConstants.Fail, strFailureMessage)
            End If

        End If

        Return blnSuccess

    End Function

    Protected Sub ReportManagerErrorCleanup(ByVal eMgrCleanupActionCode As eCleanupActionCodeConstants)
        ReportManagerErrorCleanup(eMgrCleanupActionCode, String.Empty)
    End Sub

    Protected Sub ReportManagerErrorCleanup(ByVal eMgrCleanupActionCode As eCleanupActionCodeConstants, ByVal strFailureMessage As String)

        Dim MyConnection As System.Data.SqlClient.SqlConnection
        Dim MyCmd As New System.Data.SqlClient.SqlCommand
        Dim RetVal As Integer

        Try
            If strFailureMessage Is Nothing Then strFailureMessage = String.Empty

            MyConnection = New System.Data.SqlClient.SqlConnection(mMgrConfigDBConnectionString)
            MyConnection.Open()

            'Set up the command object prior to SP execution
            With MyCmd
                .CommandType = CommandType.StoredProcedure
                .CommandText = SP_NAME_REPORTMGRCLEANUP
                .Connection = MyConnection

                .Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
                .Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

                .Parameters.Add(New SqlClient.SqlParameter("@ManagerName", SqlDbType.VarChar, 128))
                .Parameters.Item("@ManagerName").Direction = ParameterDirection.Input
                .Parameters.Item("@ManagerName").Value = mManagerName

                .Parameters.Add(New SqlClient.SqlParameter("@State", SqlDbType.Int))
                .Parameters.Item("@State").Direction = ParameterDirection.Input
                .Parameters.Item("@State").Value = eMgrCleanupActionCode

                .Parameters.Add(New SqlClient.SqlParameter("@FailureMsg", SqlDbType.VarChar, 512))
                .Parameters.Item("@FailureMsg").Direction = ParameterDirection.Input
                .Parameters.Item("@FailureMsg").Value = strFailureMessage

                .Parameters.Add(New SqlClient.SqlParameter("@message", SqlDbType.VarChar, 512))
                .Parameters.Item("@message").Direction = ParameterDirection.Output
                .Parameters.Item("@message").Value = String.Empty
            End With

            'Execute the SP
            RetVal = MyCmd.ExecuteNonQuery

        Catch ex As System.Exception
            If mMgrConfigDBConnectionString Is Nothing Then mMgrConfigDBConnectionString = String.Empty
            Dim strErrorMessage As String = "Exception calling " & SP_NAME_REPORTMGRCLEANUP & " in ReportManagerErrorCleanup with connection string " & mMgrConfigDBConnectionString
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage & ex.Message)
        End Try

    End Sub

End Class
