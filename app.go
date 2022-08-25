package main

import (
	"log"
	"os"
	"spark-api/controllers"

	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()
	logFile, _ := os.OpenFile("./kafka.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

	sarama.Logger = log.New(logFile, "[Sarama]", log.Lshortfile|log.Ldate|log.Ltime)

	v1 := router.Group("/api/v1")
	{
		v1.GET("/sparkapp/:app", controllers.SparkAppControllerHandler())
		v1.GET("/sparkapp/", controllers.SparkAppControllerHandler())
	}

	router.NoRoute(func(c *gin.Context) {
		c.JSON(404, gin.H{"msg": "Not found"})
	})

	router.Run(":8087")

}
