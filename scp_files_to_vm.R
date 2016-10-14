#### Settings ####
username <- "user01";
password <- "mapr";
# password <- .rs.askForPassword(paste("Password for:   ", username));

transfer_data_files <- FALSE;
transfer_script_files <- TRUE;

remote_data_folder <- paste0("/user/", username, "/data/");
remote_scripts_folder <- paste0("/user/", username, "/scripts/");

scp_exe <- "D:\\Program Files (x86)\\putty\\PSCP.EXE";


#### Copy Files ####

if (transfer_data_files) {
  system2(  scp_exe
          , args = c(
                      "-sftp"
                    , "-P", "2222"
                    , "-pw", password
                    , "-r", paste0(getwd(), "/data/")
                    , paste0(username, "@127.0.0.1:", remote_data_folder)
                    )
          , input = "y" # to accept the host key
  )
}

if (transfer_script_files) {
  system2(  scp_exe
          , args = c(
                      "-sftp"
                    , "-P", "2222"
                    , "-pw", password
                    , "-r", paste0(getwd(), "/scripts/")
                    , paste0(username, "@127.0.0.1:", remote_scripts_folder)
                    )
          , input = "y" # to accept the host key
  )
}
