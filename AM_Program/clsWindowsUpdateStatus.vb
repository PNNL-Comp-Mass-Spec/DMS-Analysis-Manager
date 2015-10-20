Imports System.Runtime.InteropServices

Public Class clsWindowsUpdateStatus

    ''' <summary>
    ''' Checks whether Windows Updates are expected to occur close to the current time of day
    ''' </summary>
    ''' <returns>True if Windows updates are likely pending on this computer or the Windows servers</returns>
    ''' <remarks></remarks>
    Public Shared Function UpdatesArePending() As Boolean
        Dim pendingWindowsUpdateMessage As String = Nothing
        Return UpdatesArePending(DateTime.Now, pendingWindowsUpdateMessage)
    End Function

	''' <summary>
	''' Checks whether Windows Updates are expected to occur close to the current time of day
	''' </summary>
	''' <param name="pendingWindowsUpdateMessage">Output: description of the pending or recent Windows updates</param>
    ''' <returns>True if Windows updates are likely pending on this computer or the Windows servers</returns>
    ''' <remarks></remarks>
	Public Shared Function UpdatesArePending(<Out()> ByRef pendingWindowsUpdateMessage As String) As Boolean
		Return UpdatesArePending(DateTime.Now, pendingWindowsUpdateMessage)
	End Function

	''' <summary>
	''' Checks whether Windows Updates are expected to occur close to the current time of day
	''' </summary>
	''' <param name="currentTime">Current time of day</param>
	''' <param name="pendingWindowsUpdateMessage">Output: description of the pending or recent Windows updates</param>
    ''' <returns>True if Windows updates are likely pending on this computer or the Windows servers</returns>
    ''' <remarks></remarks>
	Public Shared Function UpdatesArePending(ByVal currentTime As DateTime, <Out()> ByRef pendingWindowsUpdateMessage As String) As Boolean

		pendingWindowsUpdateMessage = "No pending update"

		' Determine the second Tuesday in the current month
		Dim firstTuesdayInMonth = New DateTime(currentTime.Year, currentTime.Month, 1)
		While firstTuesdayInMonth.DayOfWeek <> DayOfWeek.Tuesday
			firstTuesdayInMonth = firstTuesdayInMonth.AddDays(1)
		End While

		Dim secondTuesdayInMonth = firstTuesdayInMonth.AddDays(7)

		' Windows 7 / Windows 8 Pubs install updates around 3 am on the Thursday after the second Tuesday of the month
		' Do not request a job between 12 am and 6 am on Thursday in the week with the second Tuesday of the month
		Dim dtExclusionStart = secondTuesdayInMonth.AddDays(2)
		Dim dtExclusionEnd = secondTuesdayInMonth.AddDays(2).AddHours(6)

		If currentTime >= dtExclusionStart AndAlso currentTime < dtExclusionEnd Then
			Dim dtPendingUpdateTime = secondTuesdayInMonth.AddDays(2).AddHours(3)

			If currentTime < dtPendingUpdateTime Then
				pendingWindowsUpdateMessage = "Processing boxes are expected to install Windows updates around " & dtPendingUpdateTime.ToString("hh:mm:ss tt")
			Else
				pendingWindowsUpdateMessage = "Processing boxes should have installed Windows updates at " & dtPendingUpdateTime.ToString("hh:mm:ss tt")
			End If

			Return True
		End If

		' Windows servers install updates around either 3 am or 10 am on the Sunday after the second Tuesday of the month
		' Do not request a job between 2 am and 4 am or between 9 am and 11 am on Sunday in the week with the second Tuesday of the month
		dtExclusionStart = secondTuesdayInMonth.AddDays(5).AddHours(2)
		dtExclusionEnd = secondTuesdayInMonth.AddDays(5).AddHours(4)

		Dim dtExclusionStart2 = secondTuesdayInMonth.AddDays(5).AddHours(9)
		Dim dtExclusionEnd2 = secondTuesdayInMonth.AddDays(5).AddHours(11)

        If (currentTime >= dtExclusionStart AndAlso currentTime < dtExclusionEnd) OrElse
           (currentTime >= dtExclusionStart2 AndAlso currentTime < dtExclusionEnd2) Then

            Dim dtPendingUpdateTime1 = secondTuesdayInMonth.AddDays(5).AddHours(3)
            Dim dtPendingUpdateTime2 = secondTuesdayInMonth.AddDays(5).AddHours(10)

            Dim pendingUpdateTimeText = dtPendingUpdateTime1.ToString("hh:mm:ss tt") & " or " & dtPendingUpdateTime2.ToString("hh:mm:ss tt")

            If currentTime < dtPendingUpdateTime2 Then
                pendingWindowsUpdateMessage = "Servers are expected to install Windows updates around " & pendingUpdateTimeText
            Else
                pendingWindowsUpdateMessage = "Servers should have installed Windows updates around " & pendingUpdateTimeText
            End If

            Return True
        End If

		Return False

	End Function
End Class
