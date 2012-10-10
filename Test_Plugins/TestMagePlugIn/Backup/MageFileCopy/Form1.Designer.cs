namespace MageFileCopy {
    partial class Form1 {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing) {
            if (disposing && (components != null)) {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent() {
            this.splitContainer1 = new System.Windows.Forms.SplitContainer();
            this.panel1 = new System.Windows.Forms.Panel();
            this.listDisplayControl1 = new MageClientLib.ListDisplayControl();
            this.panel2 = new System.Windows.Forms.Panel();
            this.listDisplayControl2 = new MageClientLib.ListDisplayControl();
            this.fileSelectionPanel1 = new MageClientLib.FileSelectionPanel();
            this.panel3 = new System.Windows.Forms.Panel();
            this.statusPanel1 = new MageClientLib.StatusPanel();
            this.fileCopyPanel1 = new MageClientLib.FileCopyPanel();
            this.tabControl1 = new System.Windows.Forms.TabControl();
            this.QueryTabPage = new System.Windows.Forms.TabPage();
            this.jobListPanel1 = new MageClientLib.JobListPanel();
            this.JobListTabPage = new System.Windows.Forms.TabPage();
            this.jobIDListPanel1 = new MageClientLib.JobIDListPanel();
            this.DataPackageTabPage = new System.Windows.Forms.TabPage();
            this.jobDataPackagePanel1 = new MageClientLib.JobDataPackagePanel();
            this.splitContainer1.Panel1.SuspendLayout();
            this.splitContainer1.Panel2.SuspendLayout();
            this.splitContainer1.SuspendLayout();
            this.panel1.SuspendLayout();
            this.panel2.SuspendLayout();
            this.panel3.SuspendLayout();
            this.tabControl1.SuspendLayout();
            this.QueryTabPage.SuspendLayout();
            this.JobListTabPage.SuspendLayout();
            this.DataPackageTabPage.SuspendLayout();
            this.SuspendLayout();
            // 
            // splitContainer1
            // 
            this.splitContainer1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainer1.Location = new System.Drawing.Point(0, 130);
            this.splitContainer1.Name = "splitContainer1";
            this.splitContainer1.Orientation = System.Windows.Forms.Orientation.Horizontal;
            // 
            // splitContainer1.Panel1
            // 
            this.splitContainer1.Panel1.Controls.Add(this.panel1);
            this.splitContainer1.Panel1.Padding = new System.Windows.Forms.Padding(5);
            // 
            // splitContainer1.Panel2
            // 
            this.splitContainer1.Panel2.Controls.Add(this.panel2);
            this.splitContainer1.Panel2.Padding = new System.Windows.Forms.Padding(5);
            this.splitContainer1.Size = new System.Drawing.Size(1091, 508);
            this.splitContainer1.SplitterDistance = 246;
            this.splitContainer1.TabIndex = 0;
            // 
            // panel1
            // 
            this.panel1.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.panel1.Controls.Add(this.listDisplayControl1);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel1.Location = new System.Drawing.Point(5, 5);
            this.panel1.Name = "panel1";
            this.panel1.Padding = new System.Windows.Forms.Padding(5);
            this.panel1.Size = new System.Drawing.Size(1081, 236);
            this.panel1.TabIndex = 0;
            // 
            // listDisplayControl1
            // 
            this.listDisplayControl1.Accumulator = null;
            this.listDisplayControl1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.listDisplayControl1.Location = new System.Drawing.Point(5, 5);
            this.listDisplayControl1.Name = "listDisplayControl1";
            this.listDisplayControl1.Notice = "";
            this.listDisplayControl1.PageTitle = "Title";
            this.listDisplayControl1.Size = new System.Drawing.Size(1069, 224);
            this.listDisplayControl1.TabIndex = 0;
            // 
            // panel2
            // 
            this.panel2.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.panel2.Controls.Add(this.listDisplayControl2);
            this.panel2.Controls.Add(this.fileSelectionPanel1);
            this.panel2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel2.Location = new System.Drawing.Point(5, 5);
            this.panel2.Name = "panel2";
            this.panel2.Padding = new System.Windows.Forms.Padding(5);
            this.panel2.Size = new System.Drawing.Size(1081, 248);
            this.panel2.TabIndex = 0;
            // 
            // listDisplayControl2
            // 
            this.listDisplayControl2.Accumulator = null;
            this.listDisplayControl2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.listDisplayControl2.Location = new System.Drawing.Point(5, 50);
            this.listDisplayControl2.Name = "listDisplayControl2";
            this.listDisplayControl2.Notice = "";
            this.listDisplayControl2.PageTitle = "Title";
            this.listDisplayControl2.Size = new System.Drawing.Size(1069, 191);
            this.listDisplayControl2.TabIndex = 0;
            // 
            // fileSelectionPanel1
            // 
            this.fileSelectionPanel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.fileSelectionPanel1.Location = new System.Drawing.Point(5, 5);
            this.fileSelectionPanel1.Name = "fileSelectionPanel1";
            this.fileSelectionPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.fileSelectionPanel1.Size = new System.Drawing.Size(1069, 45);
            this.fileSelectionPanel1.TabIndex = 1;
            // 
            // panel3
            // 
            this.panel3.Controls.Add(this.statusPanel1);
            this.panel3.Controls.Add(this.fileCopyPanel1);
            this.panel3.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel3.Location = new System.Drawing.Point(0, 638);
            this.panel3.Name = "panel3";
            this.panel3.Size = new System.Drawing.Size(1091, 88);
            this.panel3.TabIndex = 1;
            // 
            // statusPanel1
            // 
            this.statusPanel1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.statusPanel1.Location = new System.Drawing.Point(0, 45);
            this.statusPanel1.Name = "statusPanel1";
            this.statusPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.statusPanel1.Size = new System.Drawing.Size(1091, 42);
            this.statusPanel1.TabIndex = 1;
            // 
            // fileCopyPanel1
            // 
            this.fileCopyPanel1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.fileCopyPanel1.Location = new System.Drawing.Point(0, 0);
            this.fileCopyPanel1.Name = "fileCopyPanel1";
            this.fileCopyPanel1.OutputFolder = "C:\\Data\\Junk";
            this.fileCopyPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.fileCopyPanel1.Size = new System.Drawing.Size(1091, 50);
            this.fileCopyPanel1.TabIndex = 0;
            // 
            // tabControl1
            // 
            this.tabControl1.Controls.Add(this.QueryTabPage);
            this.tabControl1.Controls.Add(this.JobListTabPage);
            this.tabControl1.Controls.Add(this.DataPackageTabPage);
            this.tabControl1.Dock = System.Windows.Forms.DockStyle.Top;
            this.tabControl1.Location = new System.Drawing.Point(0, 0);
            this.tabControl1.Name = "tabControl1";
            this.tabControl1.SelectedIndex = 0;
            this.tabControl1.Size = new System.Drawing.Size(1091, 130);
            this.tabControl1.TabIndex = 2;
            this.tabControl1.Tag = "Job_List";
            // 
            // QueryTabPage
            // 
            this.QueryTabPage.BackColor = System.Drawing.SystemColors.Control;
            this.QueryTabPage.Controls.Add(this.jobListPanel1);
            this.QueryTabPage.Location = new System.Drawing.Point(4, 22);
            this.QueryTabPage.Name = "QueryTabPage";
            this.QueryTabPage.Padding = new System.Windows.Forms.Padding(3);
            this.QueryTabPage.Size = new System.Drawing.Size(1083, 104);
            this.QueryTabPage.TabIndex = 0;
            this.QueryTabPage.Tag = "Query";
            this.QueryTabPage.Text = "Query";
            // 
            // jobListPanel1
            // 
            this.jobListPanel1.BackColor = System.Drawing.SystemColors.Control;
            this.jobListPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.jobListPanel1.Location = new System.Drawing.Point(3, 3);
            this.jobListPanel1.Name = "jobListPanel1";
            this.jobListPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.jobListPanel1.Size = new System.Drawing.Size(1077, 98);
            this.jobListPanel1.TabIndex = 0;
            // 
            // JobListTabPage
            // 
            this.JobListTabPage.BackColor = System.Drawing.SystemColors.Control;
            this.JobListTabPage.Controls.Add(this.jobIDListPanel1);
            this.JobListTabPage.Location = new System.Drawing.Point(4, 22);
            this.JobListTabPage.Name = "JobListTabPage";
            this.JobListTabPage.Padding = new System.Windows.Forms.Padding(3);
            this.JobListTabPage.Size = new System.Drawing.Size(1083, 104);
            this.JobListTabPage.TabIndex = 1;
            this.JobListTabPage.Tag = "Job_ID_List";
            this.JobListTabPage.Text = "Job List";
            // 
            // jobIDListPanel1
            // 
            this.jobIDListPanel1.BackColor = System.Drawing.SystemColors.Control;
            this.jobIDListPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.jobIDListPanel1.Location = new System.Drawing.Point(3, 3);
            this.jobIDListPanel1.Name = "jobIDListPanel1";
            this.jobIDListPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.jobIDListPanel1.Size = new System.Drawing.Size(1077, 98);
            this.jobIDListPanel1.TabIndex = 0;
            // 
            // DataPackageTabPage
            // 
            this.DataPackageTabPage.BackColor = System.Drawing.SystemColors.Control;
            this.DataPackageTabPage.Controls.Add(this.jobDataPackagePanel1);
            this.DataPackageTabPage.Location = new System.Drawing.Point(4, 22);
            this.DataPackageTabPage.Name = "DataPackageTabPage";
            this.DataPackageTabPage.Padding = new System.Windows.Forms.Padding(3);
            this.DataPackageTabPage.Size = new System.Drawing.Size(1083, 104);
            this.DataPackageTabPage.TabIndex = 2;
            this.DataPackageTabPage.Tag = "Data_Package";
            this.DataPackageTabPage.Text = "Data Package";
            // 
            // jobDataPackagePanel1
            // 
            this.jobDataPackagePanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.jobDataPackagePanel1.Location = new System.Drawing.Point(3, 3);
            this.jobDataPackagePanel1.Name = "jobDataPackagePanel1";
            this.jobDataPackagePanel1.Padding = new System.Windows.Forms.Padding(5);
            this.jobDataPackagePanel1.Size = new System.Drawing.Size(1077, 98);
            this.jobDataPackagePanel1.TabIndex = 0;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1091, 726);
            this.Controls.Add(this.splitContainer1);
            this.Controls.Add(this.panel3);
            this.Controls.Add(this.tabControl1);
            this.Name = "Form1";
            this.Text = "Mage File Copy";
            this.splitContainer1.Panel1.ResumeLayout(false);
            this.splitContainer1.Panel2.ResumeLayout(false);
            this.splitContainer1.ResumeLayout(false);
            this.panel1.ResumeLayout(false);
            this.panel2.ResumeLayout(false);
            this.panel3.ResumeLayout(false);
            this.tabControl1.ResumeLayout(false);
            this.QueryTabPage.ResumeLayout(false);
            this.JobListTabPage.ResumeLayout(false);
            this.DataPackageTabPage.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.SplitContainer splitContainer1;
        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.Panel panel2;
        private System.Windows.Forms.Panel panel3;
        private MageClientLib.StatusPanel statusPanel1;
        private MageClientLib.FileCopyPanel fileCopyPanel1;
        private MageClientLib.ListDisplayControl listDisplayControl1;
        private MageClientLib.ListDisplayControl listDisplayControl2;
        private MageClientLib.FileSelectionPanel fileSelectionPanel1;
        private System.Windows.Forms.TabControl tabControl1;
        private System.Windows.Forms.TabPage QueryTabPage;
        private MageClientLib.JobListPanel jobListPanel1;
        private System.Windows.Forms.TabPage JobListTabPage;
        private MageClientLib.JobIDListPanel jobIDListPanel1;
        private System.Windows.Forms.TabPage DataPackageTabPage;
        private MageClientLib.JobDataPackagePanel jobDataPackagePanel1;
    }
}

