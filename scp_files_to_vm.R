#### Settings ####
username <- "user01";
# username <- "mapr";
password <- "mapr";
# password <- .rs.askForPassword(paste("Password for:   ", username));

transfer_data_files <- FALSE;
transfer_script_files <- TRUE;

# usersFolder <- "/home"
usersFolder <- "/user"
# usersFolder <- "/mapr/cluster.mapr-u16/user"

local_data_folder     <- paste(getwd(), "data", sep="/")
local_scripts_folder  <- paste(getwd(), "scripts", sep="/")
remote_data_folder    <- paste(usersFolder, username, "data", sep="/");
remote_scripts_folder <- paste(usersFolder, username, "scripts", sep="/");

remote_host <- "127.0.0.1"
remote_port <- "2222"

plink_exe <- "D:\\Program Files (x86)\\putty\\PLINK.EXE";
scp_exe <- "D:\\Program Files (x86)\\putty\\PSCP.EXE";


#### Copy Files ####

recursively_copy_folder <- function(username, password, src_folder, tgt_folder, tgt_host, tgt_port) {
  system2(  plink_exe
          , args = c( "-ssh"
                    , "-P", tgt_port
                    , "-pw", password
                    , paste0(username, "@", tgt_host)
                    , paste0("\"mkdir -p ", tgt_folder, "\"")
         ))
  system2(  scp_exe
          , args = c( "-sftp"
                    , "-P", tgt_port
                    , "-pw", password
                    , "-r", paste0(src_folder, "/")
                    , paste0(username, "@", tgt_host, ":", tgt_folder, "/")
            )
          , input = "y" # to accept the host key
  )
}

if (transfer_data_files) {
  recursively_copy_folder(  username
                          , password
                          , local_data_folder
                          , remote_data_folder
                          , remote_host
                          , remote_port
  )
}

if (transfer_script_files) {
  recursively_copy_folder(  username
                          , password
                          , local_scripts_folder
                          , remote_scripts_folder
                          , remote_host
                          , remote_port
  )
}
