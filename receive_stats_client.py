                                                "read_bytes_samples":stat["read_bytes"]["samples"],
                                                "read_bytes_min":stat["read_bytes"]["min"],
                                                "read_bytes_max":stat["read_bytes"]["max"],
                                                "read_bytes_sum":stat["read_bytes"]["sum"],
                                                "write_bytes_samples":stat["write_bytes"]["samples"],
                                                "write_bytes_min":stat["write_bytes"]["min"],
                                                "write_bytes_max":stat["write_bytes"]["max"],
                                                "write_bytes_sum":stat["write_bytes"]["sum"],
                                                "getattr":stat["getattr"]["samples"],
                                                "setattr":stat["setattr"]["samples"],
                                                "punch":stat["punch"]["samples"],
                                                "sync":stat["sync"]["samples"],
                                                "destroy":stat["destroy"]["samples"],
                                                "create":stat["create"]["samples"],
                                                "statfs":stat["statfs"]["samples"],
                                                "get_info":stat["get_info"]["samples"],
                                                "set_info":stat["set_info"]["samples"],
                                                "quotactl":stat["quotactl"]["samples"]
                                        }
                                }
                        ]
                        client.write_points(json_body)


        def callback(ch, method, properties, body):
                print(" [x] Received %r " % body)
                body=body.decode()
                li=body.split('=')
                dct=yaml.safe_load(li[1])
                if dct.get("job_stats") is None:
                        print("no job stats found")
                if dct.get("job_stats") is not None:
                        print(len(dct["job_stats"]))
                        if "MDT" in li[0]:
                                store_mdt_stats(dct)
                        if "OST" in li[0]:
                                store_ost_stats(dct)

        print("before consuming")
        channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)


        print(" [*] waiting for messages")
        channel.start_consuming()

if __name__ == '__main__':
        main()

                          
