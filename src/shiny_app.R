library(shiny)
library(ggplot2)
library(dplyr)
library(lubridate)
library(DBI)
library(RSQLite)

# Helper function to read data
read_data <- function(datastore_type, filepath) {
  if (datastore_type == "csv") {
    data <- read.csv(filepath)
  } else if (datastore_type == "sqlite") {
    con <- dbConnect(SQLite(), dbname = filepath)
    data <- dbGetQuery(con, "SELECT * FROM forecast_data")
    dbDisconnect(con)
  } else {
    stop("Unsupported datastore type")
  }
  data
}

# Filepath and datastore type settings
datastore_type <- "csv"  # Change to "sqlite" if needed
filepath <- Sys.getenv("FORECAST_FILE_LOCATION", "/home/cdsw/forecast_results.csv")

# Load data
df <- read_data(datastore_type, filepath)

# Ensure Date is in Date format
df$Date <- as.Date(df$Date)

# Compute the quarter for each date
df$Quarter <- paste0(year(df$Date), "-Q", quarter(df$Date))



ui <- fluidPage(
  titlePanel("Forecasts(Volume & $Sales) for Liquor Categories"),
  
  sidebarLayout(
    sidebarPanel(
      selectInput("category", "Select Category", choices = unique(df$category_name), selected = "American Schnapps"),
      selectInput("quarter", "Select Quarter", choices = c(unique(df$Quarter), "Full Forecast"), selected = "Full Forecast"),
      selectInput("store", "Select Store", choices = c("All Stores", unique(df$store_number)), selected = "All Stores"),
    ),
    
    mainPanel(
      tabsetPanel(
        tabPanel("Volume Forecast",
                 plotOutput("volume_plot")),
        tabPanel("Sales Forecast",
                 plotOutput("sales_plot"))
      )
    )
  )
)

# Server
server <- function(input, output, session) {
  
  filtered_data <- reactive({
    if (input$store == "All Stores") {
      df_filtered <- df %>% filter(category_name == input$category)
    } else {
      df_filtered <- df %>% filter(category_name == input$category, store_number == input$store)
    }
    if (input$quarter == "Full Forecast") {
      df_filtered
    } else {
      df_filtered %>% filter(Quarter == input$quarter)
    }
  })
  
  output$volume_plot <- renderPlot({
    req(filtered_data())
    ggplot(filtered_data(), aes(x = Date, y = forecast_volume)) +
      geom_line() +
      geom_point() +
      labs(title = paste("Volume Forecast for", input$category, ifelse(input$quarter == "Full Forecast", "", paste("in", input$quarter))),
           x = "Date", y = "Forecast Volume")
  })
  
  output$sales_plot <- renderPlot({
    req(filtered_data())
    ggplot(filtered_data(), aes(x = Date, y = fc_sales)) +
      geom_line() +
      geom_point() +
      labs(title = paste("Sales Forecast for", input$category, ifelse(input$quarter == "Full Forecast", "", paste("in", input$quarter))),
           x = "Date", y = "Forecast Sales")
  })
  
  
}

shinyApp(ui=ui, server=server, options=list(host="127.0.0.1", port=as.integer(Sys.getenv("CDSW_READONLY_PORT"))))