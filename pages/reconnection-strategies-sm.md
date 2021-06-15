Reconnection Strategies <!-- add heading -->

<!-- explain what a reconnection strategy is --> A reconnection strategy allows you to configure how many times a reconnection is attempted and how long to wait between attempts. 

When a Mule app <!-- or "application"? --> starts, the application <!-- or Mule? --> runs a test on components, such as the HTTP or FTP connections to an external server. By default when the test fails, the application logs a warning message and continues with the deployment. You can change the behavior of a failed connection by modifying the settings for the reconnection frequency and the number reconnection attempts. The configurable settings are:  <!-- I would put the settings in a table -->
<!-- example only -->
| Setting Name | Description | Values |
| ----------- | ----------- | ----------- |
| Fail Deployment (failsDeployment) | This setting forces the deployment to fail if the connection fails and the configured number of reconnect attempts are exhausted. | true (default) or false | 
| Reconnect (reconnect) | This setting limits the number of reconnection attempts and the interval at which to execute them. The attributes are **frequency** and **count**. | frequency="2000" (default ms) and count="2" (default) |
| Reconnect Forever (reconnect-forever) | This setting attempts to reconnect at a given interval for as long as the app runs. | frequency="2000" (default ms) **Note** You do not need to specify count because there is no limit in the number of retries. 

This is an example of a fail after 5 reconnection attempts at 4000 ms intervals are exhausted: 

`
<http:request-config name="HTTP_Request_Config" doc:name="HTTP Request Config">
  <http:request-conection host="https://jsonplaceholder.typicode.com/posts" port="80" >
    <reconnection failsDeployment="true" >
      <reconnect frequency="4000" count="5" />
    </reconnection>
  </http:request-connection>
</http:request-config>
`

This is an example of no limit on reconnection attempts which are at 4000 ms intervals: 

`
<http:request-config name="HTTP_Request_Config" doc:name="HTTP Request Config">
  <http:request-connection host="https://jsonplaceholder.typicode.com/posts" port="80" >
    <reconnection> <!-- or should it be "<reconnection failsDeployment="true"? -->
      <reconnect-forever frequency="4000" />
    </reconnection>
  </http:request-connection>
</http:request-config>
`
<!-- [source,xml] are the examples supposed to be in XML? -->

