namespace MageFileContents {
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
            this.directorySearcher1 = new System.DirectoryServices.DirectorySearcher();
            this.panel2 = new System.Windows.Forms.Panel();
            this.fileContentFilterPanel1 = new MageClientLib.FileContentFilterPanel();
            this.statusPanel1 = new MageClientLib.StatusPanel();
            this.panel3 = new System.Windows.Forms.Panel();
            this.listDisplayControl1 = new MageClientLib.ListDisplayControl();
            this.localFolderPanel1 = new MageClientLib.LocalFolderPanel();
            this.panel2.SuspendLayout();
            this.panel3.SuspendLayout();
            this.SuspendLayout();
            // 
            // directorySearcher1
            // 
            this.directorySearcher1.ClientTimeout = System.TimeSpan.Parse("-00:00:01");
            this.directorySearcher1.ServerPageTimeLimit = System.TimeSpan.Parse("-00:00:01");
            this.directorySearcher1.ServerTimeLimit = System.TimeSpan.Parse("-00:00:01");
            // 
            // panel2
            // 
            this.panel2.Controls.Add(this.fileContentFilterPanel1);
            this.panel2.Controls.Add(this.statusPanel1);
            this.panel2.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel2.Location = new System.Drawing.Point(0, 414);
            this.panel2.Name = "panel2";
            this.panel2.Size = new System.Drawing.Size(788, 120);
            this.panel2.TabIndex = 9;
            // 
            // fileContentFilterPanel1
            // 
            this.fileContentFilterPanel1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.fileContentFilterPanel1.Location = new System.Drawing.Point(5, 5);
            this.fileContentFilterPanel1.Name = "fileContentFilterPanel1";
            this.fileContentFilterPanel1.OutputFolder = "C:\\Data\\jango";
            this.fileContentFilterPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.fileContentFilterPanel1.Size = new System.Drawing.Size(778, 75);
            this.fileContentFilterPanel1.TabIndex = 9;
            // 
            // statusPanel1
            // 
            this.statusPanel1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.statusPanel1.Location = new System.Drawing.Point(5, 75);
            this.statusPanel1.Name = "statusPanel1";
            this.statusPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.statusPanel1.Size = new System.Drawing.Size(778, 42);
            this.statusPanel1.TabIndex = 8;
            // 
            // panel3
            // 
            this.panel3.Controls.Add(this.listDisplayControl1);
            this.panel3.Dock = System.Windows.Forms.DockStyle.Fill;
            this.panel3.Location = new System.Drawing.Point(0, 80);
            this.panel3.Name = "panel3";
            this.panel3.Padding = new System.Windows.Forms.Padding(5);
            this.panel3.Size = new System.Drawing.Size(788, 334);
            this.panel3.TabIndex = 10;
            // 
            // listDisplayControl1
            // 
            this.listDisplayControl1.Accumulator = null;
            this.listDisplayControl1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.listDisplayControl1.Location = new System.Drawing.Point(5, 5);
            this.listDisplayControl1.Name = "listDisplayControl1";
            this.listDisplayControl1.Notice = "";
            this.listDisplayControl1.PageTitle = "Title";
            this.listDisplayControl1.Size = new System.Drawing.Size(778, 324);
            this.listDisplayControl1.TabIndex = 6;
            // 
            // localFolderPanel1
            // 
            this.localFolderPanel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.localFolderPanel1.FileNameFilter = "syn.txt";
            this.localFolderPanel1.Folder = "C:\\Data\\syn";
            this.localFolderPanel1.Location = new System.Drawing.Point(0, 0);
            this.localFolderPanel1.Name = "localFolderPanel1";
            this.localFolderPanel1.Padding = new System.Windows.Forms.Padding(5);
            this.localFolderPanel1.Size = new System.Drawing.Size(788, 80);
            this.localFolderPanel1.TabIndex = 11;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(788, 534);
            this.Controls.Add(this.panel3);
            this.Controls.Add(this.localFolderPanel1);
            this.Controls.Add(this.panel2);
            this.Name = "Form1";
            this.Text = "Mage - File Filtering";
            this.panel2.ResumeLayout(false);
            this.panel3.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.DirectoryServices.DirectorySearcher directorySearcher1;
        private MageClientLib.ListDisplayControl listDisplayControl1;
        private MageClientLib.StatusPanel statusPanel1;
        private System.Windows.Forms.Panel panel2;
        private MageClientLib.FileContentFilterPanel fileContentFilterPanel1;
        private System.Windows.Forms.Panel panel3;
        private MageClientLib.LocalFolderPanel localFolderPanel1;
    }
}

