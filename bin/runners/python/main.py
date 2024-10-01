#
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import requests

BEARER_TOKEN = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJKdXJpUGV0ZXJzZW4jODg4OCIsImxhc3ROYW1lIjoiUGV0ZXJzZW4iLCJyb2xlIjoiVVNFUiIsInNlcnZpY2VMaWNlbnNlQWdyZWVtZW50Ijp0cnVlLCJpc3MiOiJjbG91ZC5zZHUuZGsiLCJmaXJzdE5hbWVzIjoiSnVyaSIsImV4dGVuZGVkQnlDaGFpbiI6W10sIm9yZ0lkIjoiaXR1LmRrIiwidWlkIjo0MiwiYXVkIjoiYWxsOndyaXRlIiwidHdvRmFjdG9yQXV0aGVudGljYXRpb24iOnRydWUsImV4cCI6MTcyNzc4MDg5OCwiaWF0IjoxNzI3NzgwMjk4LCJlbWFpbCI6Imp1cnBAaXR1LmRrIiwicHJpbmNpcGFsVHlwZSI6IndheWYiLCJwdWJsaWNTZXNzaW9uUmVmZXJlbmNlIjoiMmIwZmVhMmMtZjg4OS00OGEzLTkyMDEtMDIxNzQzMjJmYjdhIn0.SIRm1qYTRFb3G_m2j4-Z23VKvw1V-rT99PM4vvyQcIMV-3gi3SUwbFl62CwQrLMDlespyzIbgvVeacS1_I51KdrKvYyPa-dXya1e2fSc_0mD93l_C8AWf--hIglGejuA6xcSEi3Z4PCRJdnrQvTagBA5GH9baXMnx4eAkZwN22KQAnrrfvgjVw8IxDVsDPBpThELFB8eX9siG5AJW-f9G5r62kJdUABOQCkkJxX-OyIboEO3oxh3TT0iOr8vqMOBbzsJsl41bHvCQwB4dNMRSEATCkd8KZb_a9xvel6rYOYMZLbwDyPBTVcoCw2GkRrGa5STadByhy21tXZNZEcM5g"

API_URL = "https://cloud.sdu.dk/api"
JOBS_URL = API_URL + "/jobs/browse?providers=ucloud&state=RUNNING"
POSTGRES_PRODUCT_URL = API_URL + "/hpc/apps/byNameAndVersion?appName=postgresql"

JOB_CREATION_URL = API_URL + "/jobs"
PRODUCT_NAME = "u1-standard-1"

POSTGRES_BODY = {
    "items": [
            {
                "allowDuplicateJob": False,
                "application": {
                    "authors": [
                        "PostgreSQL Global Development Group"
                    ],
                    "createdAt": 1723619869455,
                    "description": "PostgreSQL is an object-relational database management system.\n",
                    "flavorName": None,
                    "group": {
                        "metadata": {
                            "id": 67
                        },
                        "specification": {
                            "categories": [
                                26
                            ],
                            "colorReplacement": {
                                "dark": {
                                    "0": 16728319
                                },
                                "light": None
                            },
                            "defaultFlavor": "postgresql",
                            "description": "PostgreSQL is an object-relational database management system.",
                            "logoHasText": True,
                            "title": "PostgreSQL Server"
                        },
                        "status": {
                            "applications": [
                                {
                                    "favorite": False,
                                    "metadata": {
                                        "authors": [
                                            "PostgreSQL Global Development Group"
                                        ],
                                        "createdAt": 1723619869455,
                                        "description": "PostgreSQL is an object-relational database management system.\n",
                                        "flavorName": None,
                                        "group": {
                                            "metadata": {
                                                "id": 67
                                            },
                                            "specification": {
                                                "categories": [
                                                    26
                                                ],
                                                "colorReplacement": {
                                                    "dark": {
                                                        "0": 16728319
                                                    },
                                                    "light": None
                                                },
                                                "defaultFlavor": "postgresql",
                                                "description": "PostgreSQL is an object-relational database management system.",
                                                "logoHasText": True,
                                                "title": "PostgreSQL Server"
                                            },
                                            "status": {
                                                "applications": None
                                            }
                                        },
                                        "name": "postgresql",
                                        "public": True,
                                        "title": "PostgreSQL Server",
                                        "version": "16.4",
                                        "website": "https://docs.cloud.sdu.dk/Apps/postgresql.html"
                                    },
                                    "tags": []
                                }
                            ]
                        }
                    },
                    "name": "postgresql",
                    "public": True,
                    "title": "PostgreSQL Server",
                    "version": "16.4",
                    "website": "https://docs.cloud.sdu.dk/Apps/postgresql.html"
                },
                "name": "Postgres Test",
                "parameters": {
                    "database": {
                        "path": "/911975/lsbo-paper/postgres",
                        "readOnly": False,
                        "type": "file"
                    }
                },
                "product": {
                    "category": "u1-standard-h",
                    "id": "u1-standard-1",
                    "provider": "ucloud"
                },
                "replicas": 1,
                "resources": [
                    {
                        "path": "/911975/lsbo-paper/data",
                        "readOnly": False,
                        "type": "file"
                    }
                ],
                "sshEnabled": False,
                "timeAllocation": {
                    "hours": 1,
                    "minutes": 0,
                    "seconds": 0
                }
            }
        ]
    }

SPARK_BODY = {
	"items": [
		{
			"allowDuplicateJob": False,
			"application": {
				"authors": [
					"Matei Zaharia",
					"SDU eScience"
				],
				"createdAt": 1723468304355,
				"description": "Apache Spark Standalone Cluster.\n",
				"flavorName": None,
				"group": {
					"metadata": {
						"id": 18
					},
					"specification": {
						"categories": [
							26
						],
						"colorReplacement": {
							"dark": {
								"3947070": 16777215
							},
							"light": None
						},
						"defaultFlavor": "spark-cluster",
						"description": "Apache Spark Standalone Cluster.",
						"logoHasText": False,
						"title": "Spark Cluster"
					},
					"status": {
						"applications": [
							{
								"favorite": False,
								"metadata": {
									"authors": [
										"Matei Zaharia",
										"SDU eScience"
									],
									"createdAt": 1723468304355,
									"description": "Apache Spark Standalone Cluster.\n",
									"flavorName": None,
									"group": {
										"metadata": {
											"id": 18
										},
										"specification": {
											"categories": [
												26
											],
											"colorReplacement": {
												"dark": {
													"3947070": 16777215
												},
												"light": None
											},
											"defaultFlavor": "spark-cluster",
											"description": "Apache Spark Standalone Cluster.",
											"logoHasText": False,
											"title": "Spark Cluster"
										},
										"status": {
											"applications": None
										}
									},
									"name": "spark-cluster",
									"public": True,
									"title": "Spark Cluster",
									"version": "3.1.2",
									"website": "https://docs.cloud.sdu.dk/Apps/spark-cluster.html"
								},
								"tags": []
							}
						]
					}
				},
				"name": "spark-cluster",
				"public": True,
				"title": "Spark Cluster",
				"version": "3.1.2",
				"website": "https://docs.cloud.sdu.dk/Apps/spark-cluster.html"
			},
			"name": "Spark test",
			"parameters": {
				"file_system": {
					"path": "/911975/lsbo-paper/data",
					"readOnly": False,
					"type": "file"
				}
			},
			"product": {
				"category": "u1-standard-h",
				"id": "u1-standard-1",
				"provider": "ucloud"
			},
			"replicas": 3,
			"resources": [
				{
					"path": "/911975/lsbo-paper",
					"readOnly": False,
					"type": "file"
				}
			],
			"sshEnabled": False,
			"timeAllocation": {
				"hours": 1,
				"minutes": 0,
				"seconds": 0
			}
		}
	]
}

def main():
    headers = {"Authorization": BEARER_TOKEN}

    postgres_product = requests.get(POSTGRES_PRODUCT_URL, headers=headers).json()

    print(postgres_product)

    postgres_creation = requests.post(JOB_CREATION_URL, json=POSTGRES_BODY, headers=headers).json()
    print(postgres_creation)

    spark_creation = requests.post(JOB_CREATION_URL, json=SPARK_BODY, headers=headers).json()
    print(spark_creation)

    postgres_id = postgres_creation["responses"][0]["id"]
    spark_id = spark_creation["responses"][0]["id"]

    WAYANG_BODY = create_wayang_body(spark_id, postgres_id)

    wayang_creation = requests.post(JOB_CREATION_URL, json=WAYANG_BODY, headers=headers).json()
    print(wayang_creation)


    #print(postgres_creation)


def create_wayang_body(spark_id, postgres_id):

    return {
        "items": [
            {
                "allowDuplicateJob": False,
                "application": {
                    "authors": [
                        "SDU eScience Center"
                    ],
                    "createdAt": 1725266523045,
                    "description": "Web terminal server based on [ttyd](https://github.com/tsl0922/ttyd) command-line tool.\n",
                    "flavorName": "Ubuntu",
                    "group": {
                        "metadata": {
                            "id": 55
                        },
                        "specification": {
                            "categories": [
                                19
                            ],
                            "colorReplacement": {
                                "dark": None,
                                "light": None
                            },
                            "defaultFlavor": "terminal-ubuntu",
                            "description": "Web terminal window based on [ttyd](https://github.com/tsl0922/ttyd) command-line tool.",
                            "logoHasText": False,
                            "title": "Terminal"
                        },
                        "status": {
                            "applications": [
                                {
                                    "favorite": False,
                                    "metadata": {
                                        "authors": [
                                            "SDU eScience Center"
                                        ],
                                        "createdAt": 1725261716281,
                                        "description": "Web terminal server based on [ttyd](https://github.com/tsl0922/ttyd) command-line tool.\n",
                                        "flavorName": "AlmaLinux",
                                        "group": {
                                            "metadata": {
                                                "id": 55
                                            },
                                            "specification": {
                                                "categories": [
                                                    19
                                                ],
                                                "colorReplacement": {
                                                    "dark": None,
                                                    "light": None
                                                },
                                                "defaultFlavor": "terminal-ubuntu",
                                                "description": "Web terminal window based on [ttyd](https://github.com/tsl0922/ttyd) command-line tool.",
                                                "logoHasText": False,
                                                "title": "Terminal"
                                            },
                                            "status": {
                                                "applications": None
                                            }
                                        },
                                        "name": "terminal-almalinux",
                                        "public": True,
                                        "title": "Terminal AlmaLinux",
                                        "version": "Sep2024",
                                        "website": "https://docs.cloud.sdu.dk/Apps/terminal.html"
                                    },
                                    "tags": []
                                },
                                {
                                    "favorite": False,
                                    "metadata": {
                                        "authors": [
                                            "Emiliano Molinaro <molinaro@imada.sdu.dk>"
                                        ],
                                        "createdAt": 1655206517633,
                                        "description": "Web terminal window based on [ttyd](https://github.com/tsl0922/ttyd) command-line tool.\n",
                                        "flavorName": "CentOS",
                                        "group": {
                                            "metadata": {
                                                "id": 55
                                            },
                                            "specification": {
                                                "categories": [
                                                    19
                                                ],
                                                "colorReplacement": {
                                                    "dark": None,
                                                    "light": None
                                                },
                                                "defaultFlavor": "terminal-ubuntu",
                                                "description": "Web terminal window based on [ttyd](https://github.com/tsl0922/ttyd) command-line tool.",
                                                "logoHasText": False,
                                                "title": "Terminal"
                                            },
                                            "status": {
                                                "applications": None
                                            }
                                        },
                                        "name": "terminal-centos",
                                        "public": True,
                                        "title": "Terminal CentOS",
                                        "version": "0.8.0",
                                        "website": "https://docs.cloud.sdu.dk/Apps/terminal.html"
                                    },
                                    "tags": []
                                },
                                {
                                    "favorite": False,
                                    "metadata": {
                                        "authors": [
                                            "SDU eScience Center"
                                        ],
                                        "createdAt": 1725261293709,
                                        "description": "Web terminal server based on [ttyd](https://github.com/tsl0922/ttyd) command-line tool.\n",
                                        "flavorName": "Debian",
                                        "group": {
                                            "metadata": {
                                                "id": 55
                                            },
                                            "specification": {
                                                "categories": [
                                                    19
                                                ],
                                                "colorReplacement": {
                                                    "dark": None,
                                                    "light": None
                                                },
                                                "defaultFlavor": "terminal-ubuntu",
                                                "description": "Web terminal window based on [ttyd](https://github.com/tsl0922/ttyd) command-line tool.",
                                                "logoHasText": False,
                                                "title": "Terminal"
                                            },
                                            "status": {
                                                "applications": None
                                            }
                                        },
                                        "name": "terminal-debian",
                                        "public": True,
                                        "title": "Terminal Debian",
                                        "version": "Sep2024",
                                        "website": "https://docs.cloud.sdu.dk/Apps/terminal.html"
                                    },
                                    "tags": []
                                },
                                {
                                    "favorite": False,
                                    "metadata": {
                                        "authors": [
                                            "SDU eScience Center"
                                        ],
                                        "createdAt": 1725266523045,
                                        "description": "Web terminal server based on [ttyd](https://github.com/tsl0922/ttyd) command-line tool.\n",
                                        "flavorName": "Ubuntu",
                                        "group": {
                                            "metadata": {
                                                "id": 55
                                            },
                                            "specification": {
                                                "categories": [
                                                    19
                                                ],
                                                "colorReplacement": {
                                                    "dark": None,
                                                    "light": None
                                                },
                                                "defaultFlavor": "terminal-ubuntu",
                                                "description": "Web terminal window based on [ttyd](https://github.com/tsl0922/ttyd) command-line tool.",
                                                "logoHasText": False,
                                                "title": "Terminal"
                                            },
                                            "status": {
                                                "applications": None
                                            }
                                        },
                                        "name": "terminal-ubuntu",
                                        "public": True,
                                        "title": "Terminal Ubuntu",
                                        "version": "Sep2024",
                                        "website": "https://docs.cloud.sdu.dk/Apps/terminal.html"
                                    },
                                    "tags": []
                                }
                            ]
                        }
                    },
                    "name": "terminal-ubuntu",
                    "public": True,
                    "title": "Terminal Ubuntu",
                    "version": "Sep2024",
                    "website": "https://docs.cloud.sdu.dk/Apps/terminal.html"
                },
                "name": "Wayang Test",
                "parameters": {
                    "b_var": {
                        "path": "/911975/lsbo-paper/runners/wordcount-runner.sh",
                        "readOnly": False,
                        "type": "file"
                    },
                    "requirements": {
                        "path": "/911975/lsbo-paper/runners/wayang-setup.sh",
                        "readOnly": False,
                        "type": "file"
                    }
                },
                "product": {
                    "category": "u1-standard-h",
                    "id": "u1-standard-1",
                    "provider": "ucloud"
                },
                "replicas": 1,
                "resources": [
                    {
                        "path": "/911975/lsbo-paper",
                        "readOnly": False,
                        "type": "file"
                    },
                    {
                        "hostname": "spark-cluster",
                        "jobId": spark_id,
                        "type": "peer"
                    },
                    {
                        "hostname": "postgres",
                        "jobId": postgres_id,
                        "type": "peer"
                    }
                ],
                "sshEnabled": False,
                "timeAllocation": {
                    "hours": 1,
                    "minutes": 0,
                    "seconds": 0
                }
            }
        ]
    }

if __name__ == "__main__":
    main()
