Rem Call this batch file, providing your password as the first parameter
pause

:Servers
psexec \\Gigasax -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\Albert -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\Pogo -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\Roadrunner -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\Porky -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\Elmer -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\Proteinseqs -u pnl\memadmin -p %1 ipconfig /flushdns
Goto Done


:SeqClusters
psexec \\Seqcluster1 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\Seqcluster2 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\Seqcluster3 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\Seqcluster4 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\Seqcluster5 -u pnl\memadmin -p %1 ipconfig /flushdns


:MashAndProto
psexec \\mash-01 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\mash-02 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\mash-03 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\mash-04 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\mash-05 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\mash-06 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\proto-1 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\proto-3 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\proto-4 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\proto-5 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\proto-6 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\proto-7 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\proto-8 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\proto-9 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\proto-10 -u pnl\memadmin -p %1 ipconfig /flushdns

psexec \\wd37447 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\wd37208 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\chemstation1326 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\daffy -u pnl\memadmin -p %1 ipconfig /flushdns
pause


:Pubs
psexec \\pub-01 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-02 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-03 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-04 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-05 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-06 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-07 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-08 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-09 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-10 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-11 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-12 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-15 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-16 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-17 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-20 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-21 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-22 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-23 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-24 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-25 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-26 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-27 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-28 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-29 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-30 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-31 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-32 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-33 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-34 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-35 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-40 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-41 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-42 -u pnl\memadmin -p %1 ipconfig /flushdns
psexec \\pub-43 -u pnl\memadmin -p %1 ipconfig /flushdns


:Done
