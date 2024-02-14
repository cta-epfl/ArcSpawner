"""Batch spawners

This file contains an abstraction layer for batch job queueing systems, and implements
Jupyterhub spawners for Torque, SLURM, and eventually others.

Common attributes of batch submission / resource manager environments will include notions of:
  * queue names, resource manager addresses
  * resource limits including runtime, number of processes, memory
  * singleuser child process running on (usually remote) host not known until runtime
  * job submission and monitoring via resource manager utilities
  * remote execution via submission of templated scripts
  * job names instead of PIDs
"""
import asyncio
import random
import subprocess
from async_generator import async_generator, yield_
import asyncssh
import time
import os
import re

from traitlets import Integer, Unicode, default
from arcspawner import BatchSpawnerRegexStates, JobStatus, format_template

class ARCSpawner(BatchSpawnerRegexStates):
    # TODO: new key on every connection
    # TODO: handle token

    # TODO: user dir persistence
    # TODO: image selection
    # TODO: different certs for different users

    @property
    def jh_base_url(self):
        return os.getenv("JH_BASE_URL", "https://jh-staging.cta.cscs.ch/")

    def env_string(self):
        env = ""
        for key, value in self.get_env().items():
            value = re.sub("http://.*?:8081/hub", self.jh_base_url + "/hub", value)
            # if key in ["JUPYTERHUB_SERVICE_PREFIX", "JUPYTERHUB_SERVICE_URL"]:
            #    value = value.replace("/user", "/hub/user")
            env += '("{}" ^@{}@)\n'.format(key, value)
        return env

    def user_to_path_fragment(self, user):
        if isinstance(user, dict):
            user = user["name"]

        return re.sub("[^0-1a-z]", "_", user.lower())

    def get_env(self):
        env = super().get_env()

        env["JUPYTER_PORT"] = str(self.port)
        env["JUPYTERHUB_BASE_URL"] = self.jh_base_url
        env["CTADS_URL"] = self.jh_base_url + "/services/downloadservice/"
        env["X509_USER_PROXY"] = os.environ.get(
            "X509_USER_PROXY", "/downloadservice-data/dcache_clientcert.crt"
        )

        if self.user.name:
            filename = self.user_to_path_fragment(self.user.name) + ".crt"
            own_certificate_file = os.path.join(
                os.environ["CTADS_CERTIFICATE_DIR"], filename
            )

            if os.path.isfile(own_certificate_file):
                env["X509_USER_PROXY"] = own_certificate_file

        return env

    @property
    def batch_script(self):
        return f"""&
                ( jobname = "session" )
                ( executable = "/usr/bin/bash" )( arguments = "run.sh" )
                ( environment = 
                    {self.env_string()}
                )
                ( inputfiles = 
                    ("run.sh" "{self.run_script_url}")
                    ("fkdata" "/etc/forwardkey")
                    ("image.sif" "{self.sif_image_url}") 
                )

                ( outputFiles = 
                    ("user-home-tar.tgz" "https://dcache.cta.cscs.ch:2880/pnfs/cta.cscs.ch/lst/users/{self.user_to_path_fragment(self.user.name)}/user-home-tar.tgz")                
                )
                    (cpuTime="4320")
                    (wallTime="4320")
                (* maximal time for the session directory to exist on the remote node, days *)
                    (lifeTime="14")
                (* memory required for the job, per count, Mbytes *)
                    (Memory="20000")
                (* disk space required for the job, Mbytes *)
                    (*Disk="100000"*)

                (priority="100")

                (count="{int(self.req_nprocs)}") 
                (countpernode="{int(self.req_nprocs)}") 

                (* (exclusiveexecution="yes") *)

                ( stdout = "stdout" )

                ( queue="normal" )

                ( join = "yes" ) 
                ( gmlog = "gmlog" ) """

    # """
    # #!/bin/bash
    # #SBATCH --output={{homedir}}/jupyterhub_slurmspawner_%j.log
    # #SBATCH --export={{keepvars}}
    # {% if partition  %}#SBATCH --partition={{partition}}
    # {% endif %}{% if runtime    %}#SBATCH --time={{runtime}}
    # {% endif %}{% if memory     %}#SBATCH --mem={{memory}}
    # {% endif %}{% if gres       %}#SBATCH --gres={{gres}}
    # {% endif %}{% if nprocs     %}#SBATCH --cpus-per-task={{nprocs}}
    # {% endif %}{% if reservation%}#SBATCH --reservation={{reservation}}
    # {% endif %}{% if options    %}#SBATCH {{options}}{% endif %}

    # set -euo pipefail

    # trap 'echo SIGTERM received' TERM
    # {{prologue}}

    # which jupyterhub-singleuser

    # {% if srun %}{{srun}} {% endif %}{{cmd}}
    # echo "jupyterhub-singleuser ended gracefully"
    # {{epilogue}}
    # """

    http_timeout = Integer(3600, config=True, help="Timeout for HTTP requests")
    start_timeout = Integer(3600, config=True)

    # all these req_foo traits will be available as substvars for templated strings
    req_cluster = Unicode(
        "",
        help="Cluster name to submit job to resource manager",
    ).tag(config=True)

    req_qos = Unicode(
        "",
        help="QoS name to submit job to resource manager",
    ).tag(config=True)

    exec_prefix = Unicode(
        "",
    ).tag(config=True)

    req_srun = Unicode(
        "srun",
        help="Set req_srun='' to disable running in job step, and note that "
        "this affects environment handling.  This is effectively a "
        "prefix for the singleuser command.",
    ).tag(config=True)

    req_reservation = Unicode(
        "",
        help="Reservation name to submit to resource manager",
    ).tag(config=True)

    req_gres = Unicode(
        "",
        help="Additional resources (e.g. GPUs) requested",
    ).tag(config=True)

    exec_prefix = Unicode(
        "",
        # "sudo -E -u {username}",
        help="Standard execution prefix (e.g. the default sudo -E -u {username})",
    ).tag(config=True)

    run_script_url = Unicode(
        "",
        # "https://dcache.cta.cscs.ch:2880/lst/software/run.sh",
        help="run_script_url",
    ).tag(config=True)

    sif_image_url = Unicode(
        "https://dcache.cta.cscs.ch:2880/lst/software/jh-lst-53e79bd3.sif",
        help="sif_image_url",
    ).tag(config=True)

    input_as_file = True

    # outputs line like "Submitted batch job 209"
    batch_submit_cmd = Unicode("arcsub -d DEBUG").tag(config=True)
    # outputs status and exec node like "RUNNING hostname"
    batch_query_cmd = Unicode("arcstat -d DEBUG {job_id}").tag(config=True)
    batch_cancel_cmd = Unicode("arckill -d DEBUG {job_id}").tag(config=True)
    # use long-form states: PENDING,  CONFIGURING = pending
    # #  RUNNING,  COMPLETING = running
    # state_pending_re = Unicode(r"^(?:Accepted|Submitted)").tag(config=True)
    # state_running_re = Unicode(r"^(?:Running)").tag(config=True)
    # state_unknown_re = Unicode(
    #     r"^_load_jobs error: (?:Socket timed out on send/recv|Unable to contact slurm controller)"
    # ).tag(config=True)
    # state_exechost_re = Unicode(r"\s+((?:[\w_-]+\.?)+)$").tag(config=True)

    def parse_job_id(self, output):
        self.log.info("output: %s", output)
        try:
            for output_line in output.splitlines():
                self.log.info("output_line: %s", output_line)
                if output_line.startswith("Job submitted with jobid:"):
                    id = output_line.split()[-1]
        except Exception as e:
            self.log.error("ARCSpawner unable to parse job ID from text: " + output)
            raise e
        return id


    async def get_arcinfo(self):
        arcinfo = subprocess.check_output(["arcinfo", "-l"]).strip()
        self.arcinfo = dict(
            free_slots=int(re.search(r"Free slots: ([0-9]*)", arcinfo.decode()).group(1)),
            total_slots=int(re.search(r"Total slots: ([0-9]*)", arcinfo.decode()).group(1)),
        )

    async def proxy_info(self):
        cmd = "arcproxy -i vomsACvalidityLeft"

        try:
            self.proxy_vomsACvalidityLeft = await self.run_command(cmd)
            self.proxy_vomsACvalidityLeft = int(self.proxy_vomsACvalidityLeft.strip())
        except RuntimeError as e:
            # e.args[0] is stderr from the process
            self.proxy_vomsACvalidityLeft = None
        except Exception as e:
            self.log.error("Error querying proxy validity: %s", e)
            self.proxy_vomsACvalidityLeft = None

    # arcinfo  -l
    
    @property
    def job_state(self):
        if self.job_status:
            if r := re.search(r"State: (.*)", self.job_status):
                return r.group(1)

    def state_ispending(self):
        # Parse results of batch_query_cmd
        # Output determined by results of self.batch_query_cmd
        # self.log.debug("\033[31mchecking pending from job_status: " + str(self.job_status) + "\033[0m")
        r = self.job_status is None or self.job_state in [
            "Accepted",
            "Submitted",
            "Queuing",
            "Preparing",
        ]
        self.log.info(
            "\033[31mstate_ispending, job state: %s pending is %s\033[0m",
            self.job_state,
            r,
        )
        return r

    def state_isrunning(self):
        # Parse results of batch_query_cmd
        # Output determined by results of self.batch_query_cmd
        r = self.job_state in ["Running"]
        self.log.info(
            "\033[31mstate_isrunning, job state: %s running is %s\033[0m",
            self.job_state,
            r,
        )
        return r

    def state_isready(self):
        if self.job_state not in ["Running"]:
            self.log.info("job state is not Running: not ready")
            return False

        if not getattr(self, "have_tunnel", False):
            self.log.info("no tunnel: not ready")
            return False

        if not hasattr(self, "host_ip"):
            self.log.info("no host_ip: not ready")
            return False

        # if not hasattr(self, 'ssh_tunnel_connection'):
        #    self.log.info("no remote ssh tunnel connection: not ready")
        #    return False

        self.log.info("job is ready")
        return True

    forward_gateway = Unicode(
        "cgw.dev.ctaodc.ch",
        help="Host used for SSH tunneling",
    ).tag(config=True)

    async def make_ssh_tunnel(self):
        self.log.info(
            "\033[32mtrying to establish ssh tunnel to %s\033[0m",
            self.forward_gateway,
            self.port,
        )
        if hasattr(self, "ssh_tunnel_connection"):
            self.log.info(
                "ssh tunnel already established: %s", self.ssh_tunnel_connection
            )
        else:
            self.log.info(
                "\033[32mestablishing ssh tunnel to %s\033[0m",
                self.forward_gateway,
                self.port,
            )
            async with asyncssh.connect(
                self.forward_gateway,
                # known_hosts="/etc/ssh_gw_known_hosts",
                known_hosts=None,
                client_keys=[
                    "/etc/forwardkey"
                    # (asyncssh.read_private_key("/etc/forwardkey"),
                    # asyncssh.read_certificate("/etc/forwardkey.pub"))
                ],
                username="forwarder",
            ) as conn:
                self.ssh_tunnel_connection = conn
                self.log.info(
                    "SSH connection established: %s will make forward to %s",
                    conn,
                    self.port,
                )
                listener = await conn.forward_local_port(
                    "0.0.0.0", self.port, "0.0.0.0", self.port
                )
                # listener = await conn.forward_local_port('127.0.0.1', self.port, '127.0.0.1', self.port)
                self.log.info(
                    "\033[31mListening %s on port %s\033[0m",
                    listener,
                    listener.get_port(),
                )
                await listener.wait_closed()
                # self.log.info("SSH connection closed: %s", conn)
                # del self.ssh_tunnel_connection

            # try:
            #     asyncio.get_event_loop().run_until_complete(run_client())
        # except (OSError, asyncssh.Error) as exc:
        #     sys.exit('SSH connection failed: ' + str(exc))

    def state_gethost(self):
        if (r := re.search(r"JPORT=(\d{4,5})", self.job_log)) is not None:
            self.anticipated_port = int(r.group(1))
            self.log.info("found anticipated port: %s", self.anticipated_port)

        r = re.search(
            r"inet (\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})/\d+? scope global hsn0",
            self.job_log,
        )
        if r is not None:
            self.host_ip = r.group(1)

        if re.search("Sending command: .*sleep 60", self.job_log):
            self.log.info("found sleep command in job log")
            self.have_tunnel = True

        server_details = re.search(
            r"http://127.0.0.1:(?P<port>\d{4})/.*?/lab(?:\?token=(?P<token>[0-9a-z]*))?",
            self.job_log,
        )

        if server_details is None:
            self.log.info(
                "no server record found in '\033[33m%s\033[0m",
                "\n".join(self.job_log.split("\n")[-30:]),
            )
            return None
        else:
            self.port = int(server_details.group("port"))
            self.server_token = server_details.group("token")

            self.log.info("found server token: %s", self.server_token)

            # return self.forward_gateway
            return "127.0.0.1"

    @default("req_homedir")
    def _req_homedir_default(self):
        return "/root"

    async def query_job_log(self):
        """Check job status, return JobStatus object."""

        cmd = f"arccat {self.job_id}"

        self.log.info("Spawner querying job: " + cmd)
        try:
            self.job_log = await self.run_command(cmd)
        except RuntimeError as e:
            # e.args[0] is stderr from the process
            self.job_log = e.args[0]
        except Exception as e:
            self.log.error("Error querying job " + self.job_id)
            self.job_log = ""

    @async_generator
    async def progress(self):
        while True:
            if self.state_ispending():
                # TODO: report cluster status and user details (dteam, etc)
                await yield_(
                    {
                        "message": (
                            f"Pending in queue, ARC status {self.job_state}"
                            f" (timeout in {int(self.start_timeout - time.time() + self.tstart)})"
                            f" Noir access valid for {self.proxy_vomsACvalidityLeft/3600:.1f}h"
                            f" current load {self.arcinfo['total_slots'] - self.arcinfo['free_slots']}/{self.arcinfo['total_slots']}"
                        )
                    }
                )
            elif self.state_isrunning():
                if self.state_isready():
                    await yield_(
                        {
                            "message": ("Cluster job started... waiting to connect"),
                        }
                    )
                    return
                else:
                    await yield_(
                        {
                            "message": (
                                "Cluster job running... waiting to see signs of life;"
                                " host "
                                + (
                                    " ASSIGNED"
                                    if hasattr(self, "host_ip")
                                    else " NOT assigned"
                                )
                                + " "
                                # f" host {getattr(self, 'host_ip', 'unknown')}"
                                " tunnel"
                                + (
                                    " READY"
                                    if getattr(self, "have_tunnel", False)
                                    else " NOT ready"
                                )
                                + " "
                                " tunnel port"
                                + (
                                    " PLANNED"
                                    if hasattr(self, "anticipated_port")
                                    else " NOT planned yet"
                                )
                            ),
                        }
                    )

            else:
                await yield_(
                    {
                        "message": "Unknown status...",
                    }
                )
            await asyncio.sleep(1)

    async def start(self):
        """Start the process"""

        await self.proxy_info()
        if self.proxy_vomsACvalidityLeft is None:
            raise RuntimeError(
                "No valid credentials to connect to ARC, aborting. Please contact support if you need it urgently."
            )        

        self.log.info("proxy validity: %s", self.proxy_vomsACvalidityLeft)        

        self.ip = self.traits()["ip"].default_value
        # self.port = self.traits()["port"].default_value

        self.port = random.randint(8700, 8999)

        self.tstart = time.time()

        if self.server:
            self.server.port = self.port

        self.log.info("\033[31mwill create tunnel task\033[0m")
        # await self.make_ssh_tunnel()
        self.ssh_tunnel_task = asyncio.create_task(self.make_ssh_tunnel())
        self.log.info("\033[31mcreated tunnel task %s\033[0m", self.ssh_tunnel_task)

        job = await self.submit_batch_script()

        # We are called with a timeout, and if the timeout expires this function will
        # be interrupted at the next yield, and self.stop() will be called.
        # So this function should not return unless successful, and if unsuccessful
        # should either raise and Exception or loop forever.
        if len(self.job_id) == 0:
            raise RuntimeError(
                "Jupyter batch job submission failure (no jobid in output)"
            )
        while True:
            self.log.info("\033[33mloop before running job\033[0m")

            await self.get_arcinfo()

            status = await self.query_job_status()
            if status == JobStatus.RUNNING:
                self.log.info(
                    "\033[33mBREAKING loop before running job since it's running\033[0m"
                )
                break
            elif status == JobStatus.PENDING:
                self.log.debug("Job " + self.job_id + " still pending")
            elif status == JobStatus.UNKNOWN:
                self.log.debug("Job " + self.job_id + " still unknown")
            else:
                self.log.warning(
                    "Job "
                    + self.job_id
                    + " neither pending nor running.\n"
                    + self.job_status
                )
                self.clear_state()
                raise RuntimeError(
                    "The Jupyter batch job has disappeared"
                    " while pending in the queue or died immediately"
                    " after starting."
                )
            await asyncio.sleep(self.startup_poll_interval)

        while True:
            self.log.info("\033[31mloop for running job\033[0m")
            await self.query_job_log()

            try:
                self.state_gethost()
                if self.port != self.traits()["port"].default_value:
                    self.log.info(f"found ip {self.ip} and port {self.port} in log")

                    if hasattr(self, "anticipated_port"):
                        self.log.info(
                            "\033[31mfound anticipated port: %s, will make remote ssh tunnel\033[0m",
                            self.anticipated_port,
                        )
                        break
                    else:
                        self.log.info(
                            "\033[31mno anticipated port yet, can not make remote tunnel\033[0m"
                        )
            except Exception as e:
                self.log.warn(f"failed to get ip: {e}")
            else:
                self.log.info("no ip in log yet")

            await asyncio.sleep(self.startup_poll_interval)

        # self.ip = "127.0.0.1" #self.state_gethost()
        self.ip = "148.187.151.63"  # self.state_gethost()
        self.port = self.anticipated_port
        self.server.port = self.anticipated_port

        while self.port == 0:
            await asyncio.sleep(self.startup_poll_interval)
            # Test framework: For testing, mock_port is set because we
            # don't actually run the single-user server yet.
            if hasattr(self, "mock_port"):
                self.port = self.mock_port

        self.db.commit()
        self.log.info(
            "Notebook server job {0} started at {1}:{2}".format(
                self.job_id, self.ip, self.port
            )
        )

        return self.ip, self.port
        # url = f"http://{self.ip}:{self.port}/lab?token={self.server_token}"

        # self.log.info("Spawner started job with full URL: " + url)

        # return url

    async def cancel_batch_job(self):
        subvars = self.get_req_subvars()
        subvars["job_id"] = self.job_id
        cmd = " ".join(
            (
                format_template(self.exec_prefix, **subvars),
                format_template(self.batch_cancel_cmd, **subvars),
            )
        )
        self.log.info("Cancelling job " + self.job_id + ": " + cmd)
        await self.run_command(cmd)
        if (self.ssh_tunnel_task):
            self.ssh_tunnel_task.cancel()
